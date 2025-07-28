# Define a method to extract work metadata and files for a specific tenant
def extract_work_metadata_and_files(tenant_cname, work_types)
  # Find the tenant by cname
  tenant = Account.find_by(cname: tenant_cname)

  if tenant.nil?
    puts "Error: No tenant found with cname: #{tenant_cname}"
    return
  end

  tenant_name = tenant.tenant # Get the actual tenant name from the account
  puts "Extracting work metadata and files for tenant: #{tenant_cname} (tenant: #{tenant_name})"

  begin
    # Switch to the tenant's schema using the actual tenant name
    Apartment::Tenant.switch!(tenant_name)

    # Open a file to write the extracted information for the entire tenant
    File.open("#{tenant_cname}_works_data.json", 'w') do |file|
      # Write the opening bracket of a JSON object
      file.puts "{"

      first_work_type = true # Track if it's the first work type for proper JSON formatting

      # Iterate through all the specified work types
      work_types.each do |work_type|
        begin
          # Get the model class for the current work type
          model_class = work_type.constantize

          # Collect data for the current work type
          work_data_list = [] # Stores data for all works of this type
          model_class.find_each do |work|
            # Extract metadata and attached files for the work
            work_data = work.attributes.merge(
              visibility: work.visibility,
              embargo: work.try(:embargo)&.attributes, # Fetch embargo details if available
              lease: work.try(:lease)&.attributes,     # Fetch lease details if available
              admin: work.try(:admin_set)&.attributes, # Fetch admin set if available
              workflow_status: work.try(:to_sipity_entity)&.workflow_state_name, # Include workflow status
              collections: work.members.select { |member| member.is_a?(Collection) }.map(&:attributes) # Fetch collections
            )

            # Handle access controls for the work
            if work.access_control_id.present?
              begin
                access_control = Hydra::AccessControl.find(work.access_control_id)
                work_data[:access_control] = access_control.attributes
                work_data[:access_control][:permissions] = access_control.permissions.map(&:attributes)
                work_data.delete('access_control_id')
              rescue => e
                puts "Warning: Could not fetch access control for work #{work.id}: #{e.message}"
              end
            end

            # Extract file metadata, including missing fields
            file_data_list = work.file_sets.map do |file_set|
              file_data = file_set.attributes.merge(
                visibility: file_set.visibility,
                embargo: file_set.try(:embargo)&.attributes,
                lease: file_set.try(:lease)&.attributes,
                file_size: file_set.original_file&.size || 0,
                digest: extract_checksum_info(file_set),
                original_file_metadata: file_set.original_file&.attributes&.except('id', 'created_at', 'updated_at') || {}
              )

              # Handle access controls for the file set
              if file_set.access_control_id.present?
                begin
                  file_access_control = Hydra::AccessControl.find(file_set.access_control_id)
                  file_data[:access_control] = {
                    permissions: file_access_control.permissions.map(&:attributes)
                  }
                  file_data.delete('access_control_id')
                rescue => e
                  puts "Warning: Could not fetch access control for file #{file_set.id}: #{e.message}"
                end
              end

              file_data
            end


            # Integrate file data into work data
            work_data[:files] = file_data_list unless file_data_list.empty?
            work_data_list << work_data
          end

          # Skip this work type if no works were found
          next if work_data_list.empty?

          # Write the work type and its data to the file
          file.puts (first_work_type ? "" : ",") + "\"#{work_type}\": ["
          file.puts work_data_list.map(&:to_json).join(",")
          file.puts "]"
          first_work_type = false

        rescue NameError
          # If the class does not exist or is not defined, warn and skip the work type
          puts "Warning: Work type #{work_type} is not defined. Skipping..."
        rescue StandardError => e
          # Log any other errors for the work type
          puts "Error processing work type #{work_type} for tenant #{tenant_cname}: #{e.message}"
        end
      end

      # Close the JSON object for the tenant
      file.puts "}"
    end

    puts "Finished extracting data for tenant: #{tenant_cname}"

  rescue StandardError => e
    # Log any unexpected errors for the tenant
    puts "Error processing tenant #{tenant_cname}: #{e.message}"
  ensure
    # Reset to the default tenant to free up memory
    Apartment::Tenant.reset

    # Trigger garbage collection to reduce memory usage
    GC.start
  end
end

def extract_checksum_info(file_set)
  begin
    return {} if file_set.original_file.nil?

    digest = file_set.original_file.digest
    puts "Debug - Digest for #{file_set.id}: #{digest.inspect}" # Debug line

    if digest.respond_to?(:first) && digest.first.present?
      digest_parts = digest.first.to_s.split(':')
      {
        original: digest.first,
        algorithm: digest_parts[0],
        message: digest_parts[1]
      }
    else
      {}
    end
  rescue StandardError => e
    puts "Warning: Error extracting checksum for file_set #{file_set.id}: #{e.message}"
    {}
  end
end


# Define the list of work types
WORK_TYPES = %w[
  AnschutzWork ArchivalMaterial Article Book BookContribution ConferenceItem Dataset DataManagementPlan DenverArticle
  DenverBook DenverBookChapter DenverDataset DenverImage DenverMap DenverMultimedia DenverPresentationMaterial
  DenverSerialPublication DenverThesisDissertationCapstone ExhibitionItem GrantRecord LabNotebook NsuGenericWork
  NsuArticle OpenEducationalResource Report ResearchMethodology Software Minute TimeBasedMedia ThesisOrDissertation
  PacificArticle PacificBook PacificImage PacificThesisOrDissertation PacificBookChapter PacificMedia PacificNewsClipping
  PacificPresentation PacificTextWork PacificUncategorized Preprint Presentation RedlandsArticle RedlandsBook
  RedlandsChaptersAndBookSection RedlandsConferencesReportsAndPaper RedlandsOpenEducationalResource RedlandsMedia
  RedlandsStudentWork UbiquityTemplateWork UnaArchivalItem UnaArticle UnaBook UnaChaptersAndBookSection UnaExhibition
  UnaImage UnaOpenEducationalResource UnaPresentation UnaThesisOrDissertation UnaTimeBasedMedia UvaWork UngArticle
  UngBook UngBookChapter UngDataset UngImage UngThesisDissertation UngTimeBasedMedia UngPresentation UngArchivalMaterial
  LtuArticle LtuBook LtuBookChapter LtuDataset LtuImage LtuPresentation LtuThesisDissertation LtuTimeBasedMedia
  LtuSerial LtuImageArtifact OkcArticle OkcBook OkcArchivalAndLegalMaterial OkcGenericWork OkcImage OkcPresentation
  OkcTimeBasedMedia OkcChaptersAndBookSection BcArticle BcBook BcArchivalAndLegalMaterial BcImage BcPresentation
  BcTimeBasedMedia BcChaptersAndBookSection LacTimeBasedMedia LacArchivalMaterial LacImage LacThesisDissertation LacBook
  EslnArticle EslnBook EslnBookChapter EslnDataset EslnThesisDissertation EslnPresentation EslnArchivalMaterial
  EslnTemplateWork GenericWork Image
].freeze

# Main execution logic
if ARGV.length != 1
  puts "Usage: ruby extract_metadata.rb <tenant_cname>"
  exit 1
end

tenant_cname = ARGV[0] # Get the tenant cname from command-line arguments

puts "Starting extraction for tenant cname: #{tenant_cname}"

extract_work_metadata_and_files(tenant_cname, WORK_TYPES)

puts "Completed extraction for tenant cname: #{tenant_cname}"
puts "======================================="