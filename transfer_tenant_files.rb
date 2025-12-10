# scripts/transfer_tenant_files.rb

require 'aws-sdk-s3'

class TenantFileTransfer
  def initialize(tenant_name, target_bucket)
    @tenant_name = tenant_name
    @target_bucket = target_bucket
    @source_bucket = ENV['AWS_BUCKET'] || Hyrax.config.upload_bucket
    @files_transferred = 0
    @failed_count = 0
    @failed_files = []
  end

  def run
    puts "="*60
    puts "Tenant File Transfer"
    puts "="*60
    puts "Tenant: #{@tenant_name}"
    puts "Source bucket: #{@source_bucket}"
    puts "Target bucket: #{@target_bucket}"
    puts "Target folder: #{@tenant_name}/"
    puts "="*60

    # Switch to the tenant's account
    Account.find_by(tenant: @tenant_name)&.switch! do
      transfer_all_files
    end

    print_summary
  end

  private

  def transfer_all_files
    puts "\nScanning for works..."

    # Get all works for this tenant
    works = find_all_works
    puts "Found #{works.count} works\n\n"

    works.each_with_index do |work, index|
      puts "[#{index + 1}/#{works.count}] Processing work: #{work.id}"

      work.file_sets.each do |file_set|
        transfer_file(file_set)
      end
    end
  end

  def find_all_works
    all_works = []
    Hyrax.config.curation_concerns.each do |work_type|
      all_works.concat(work_type.all.to_a)
    end
    all_works
  end

  def transfer_file(file_set)
    return unless file_set.original_file.present?

    original_file = file_set.original_file
    source_key = get_file_key(original_file.id)

    # All files go into a folder named after the tenant
    target_key = "#{@tenant_name}/#{File.basename(source_key)}"

    file_name = file_set.title.first || file_set.id

    begin
      s3_client.copy_object(
        bucket: @target_bucket,
        copy_source: "#{@source_bucket}/#{source_key}",
        key: target_key
      )

      @files_transferred += 1
      puts "  ✓ #{file_name}"

    rescue StandardError => e
      @failed_count += 1
      @failed_files << { file_set_id: file_set.id, file_name: file_name, error: e.message }
      puts "  ✗ Failed: #{file_name} - #{e.message}"
    end
  end

  def get_file_key(file_id)
    # Convert Fedora file ID to S3 key path
    file_id.to_s.gsub(/^\//, '').gsub(':', '/')
  end

  def s3_client
    @s3_client ||= Aws::S3::Client.new(
      region: ENV['AWS_REGION'] || 'us-east-1',
      access_key_id: ENV['AWS_ACCESS_KEY_ID'],
      secret_access_key: ENV['AWS_SECRET_ACCESS_KEY']
    )
  end

  def print_summary
    puts "\n"
    puts "="*60
    puts "Transfer Complete!"
    puts "="*60
    puts "Files transferred: #{@files_transferred}"
    puts "Failed transfers: #{@failed_count}"

    if @failed_files.any?
      puts "\nFailed files:"
      @failed_files.each do |file_info|
        puts "  - #{file_info[:file_name]} (#{file_info[:file_set_id]})"
        puts "    Error: #{file_info[:error]}"
      end
    end
    puts "="*60
  end
end

# Run the script
if ARGV.length != 2
  puts "Usage: rails runner scripts/transfer_tenant_files.rb TENANT_NAME TARGET_BUCKET"
  puts "Example: rails runner scripts/transfer_tenant_files.rb mycollege mycollege-files-bucket"
  exit 1
end

tenant_name = ARGV[0]
target_bucket = ARGV[1]

TenantFileTransfer.new(tenant_name, target_bucket).run