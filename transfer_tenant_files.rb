# scripts/download_tenant_files.rb

require 'fileutils'

class TenantFileDownloaderFromFedora
  def initialize(tenant_cname, base_path = '/app/samvera/hyrax-webapp/tmp/downloads')
    @tenant_cname = tenant_cname
    @base_path = base_path
    @files_downloaded = 0
    @failed_count = 0
    @failed_files = []
  end

  def run
    tenant = Account.find_by(cname: @tenant_cname)

    if tenant.nil?
      puts "Error: No tenant found with cname: #{@tenant_cname}"
      return
    end

    tenant_name = tenant.tenant
    @tenant_folder = File.join(@base_path, @tenant_cname.gsub('.', '_'))

    puts "="*60
    puts "Tenant File Download"
    puts "="*60
    puts "Account: #{tenant.name}"
    puts "CNAME: #{tenant.cname}"
    puts "Tenant UUID: #{tenant_name}"
    puts "GCS Bucket: #{@bucket_name}"
    puts "Download location: #{@tenant_folder}"
    puts "="*60

    # Create the tenant folder
    FileUtils.mkdir_p(@tenant_folder)
    puts "\n✓ Created folder: #{@tenant_folder}\n"

    begin
      # Switch to the tenant's schema
      Apartment::Tenant.switch!(tenant_name)

      # Also switch the endpoints
      tenant.switch!

      puts "✓ Successfully switched to tenant\n"

      download_all_files

      print_summary

    rescue => e
      puts "\n❌ Error:"
      puts "  #{e.message}"
      puts "\nBacktrace:"
      puts e.backtrace.first(10).join("\n")
    end
  end

  private

  def download_all_files
    puts "\nScanning for FileSets..."

    file_sets = FileSet.all.to_a
    puts "Found #{file_sets.count} FileSets\n\n"

    file_sets.each_with_index do |file_set, index|
      puts "[#{index + 1}/#{file_sets.count}] Processing: #{file_set.id}"
      download_file(file_set)
    end
  end

  def download_file(file_set)
    return unless file_set.original_file.present?

    original_file = file_set.original_file
    file_id = original_file.id

    # Get the GCS path from the file ID
    gcs_path = file_id.to_s

    # Create a clean filename
    file_name = sanitize_filename(file_set.title.first || file_set.id)

    # Add file extension if available
    if original_file.mime_type.present?
      extension = get_extension_from_mime_type(original_file.mime_type)
      file_name += extension unless file_name.end_with?(extension)
    end

    # Prefix with file_set ID to avoid duplicates
    local_path = File.join(@tenant_folder, "#{file_set.id}_#{file_name}")

    begin
      # Download from GCS
      download_from_gcs(gcs_path, local_path)

      @files_downloaded += 1
      puts "  ✓ #{file_name}"

    rescue StandardError => e
      @failed_count += 1
      @failed_files << {
        file_set_id: file_set.id,
        file_name: file_name,
        gcs_path: gcs_path,
        error: e.message
      }
      puts "  ✗ Failed: #{file_name} - #{e.message}"
    end
  end

  def download_from_gcs(gcs_path, local_path)
    storage = Google::Cloud::Storage.new(
      project_id: ENV['GOOGLE_PROJECT_ID'] || 'repositories-307112'
    )

    bucket = storage.bucket(@bucket_name)
    file = bucket.file(gcs_path)

    if file.nil?
      raise "File not found in GCS: #{gcs_path}"
    end

    file.download(local_path)
  end

  def sanitize_filename(filename)
    # Remove invalid characters from filename
    filename.to_s.gsub(/[^0-9A-Za-z.\-_]/, '_')
  end

  def get_extension_from_mime_type(mime_type)
    extensions = {
      'application/pdf' => '.pdf',
      'image/jpeg' => '.jpg',
      'image/png' => '.png',
      'image/gif' => '.gif',
      'image/tiff' => '.tiff',
      'application/msword' => '.doc',
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document' => '.docx',
      'application/vnd.openxmlformats-officedocument.presentationml.presentation' => '.pptx',
      'application/vnd.ms-excel' => '.xls',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' => '.xlsx',
      'text/plain' => '.txt',
      'text/csv' => '.csv',
      'video/mp4' => '.mp4',
      'audio/mpeg' => '.mp3'
    }
    extensions[mime_type] || ''
  end

  def print_summary
    puts "\n"
    puts "="*60
    puts "Download Complete!"
    puts "="*60
    puts "Files downloaded: #{@files_downloaded}"
    puts "Failed downloads: #{@failed_count}"
    puts "Location: #{@tenant_folder}"

    if @failed_files.any?
      puts "\nFailed files:"
      @failed_files.each do |file_info|
        puts "  - #{file_info[:file_name]} (#{file_info[:file_set_id]})"
        puts "    GCS path: #{file_info[:gcs_path]}"
        puts "    Error: #{file_info[:error]}"
      end
    end
    puts "="*60
  end
end

# Run the script
if ARGV.length != 1
  puts "Usage: rails runner scripts/download_tenant_files.rb TENANT_CNAME"
  puts "Example: rails runner scripts/download_tenant_files.rb dashboard.lira.bc.edu"
  exit 1
end

tenant_cname = ARGV[0]

TenantFileDownloader.new(tenant_cname).run