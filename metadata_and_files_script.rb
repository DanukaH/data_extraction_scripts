# Define a method to extract work metadata and files for a specific tenant
def extract_work_metadata_and_files(tenant_name, work_types)
  puts "Extracting work metadata and files for tenant: #{tenant_name}"

  begin
    # Switch to the tenant's schema
    Apartment::Tenant.switch!(tenant_name)

    # Open a file to write the extracted information for the entire tenant
    File.open("#{tenant_name}_works_data.json", 'w') do |file|
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
              files: work.file_sets.map do |file_set|
                file_set.attributes.merge(
                  original_file_metadata: file_set.original_file.attributes.except('id', 'created_at', 'updated_at')
                )
              end
            )
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
          puts "Error processing work type #{work_type} for tenant #{tenant_name}: #{e.message}"
        end
      end

      # Close the JSON object for the tenant
      file.puts "}"
    end

    puts "Finished extracting data for tenant: #{tenant_name}"
  rescue StandardError => e
    # Log any unexpected errors for the tenant
    puts "Error processing tenant #{tenant_name}: #{e.message}"
  ensure
    # Reset to the default tenant to free up memory
    Apartment::Tenant.reset

    # Trigger garbage collection to reduce memory usage
    GC.start
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

tenant_name = ARGV[0] # Get the tenant cname from command-line arguments

puts "Starting extraction for tenant: #{tenant_name}"

extract_work_metadata_and_files(tenant_name, WORK_TYPES)

puts "Completed extraction for tenant: #{tenant_name}"
puts "======================================="