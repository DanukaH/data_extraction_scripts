# frozen_string_literal: true

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
            embargo_block = serialize_embargo(work)
            lease_block   = serialize_lease(work)

            work_data = work.attributes.merge(
              visibility: work.visibility,
              embargo: embargo_block,
              lease: lease_block,
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

            # Compact view of effective access to help target system decisions
            work_data[:access_effective] = {
              visibility: work.visibility,
              under_embargo: embargo_block ? embargo_block[:active] : false,
              under_lease: lease_block ? lease_block[:active] : false
            }

            # Extract file metadata, including missing fields
            file_data_list = work.file_sets.map do |file_set|
              fs_embargo = serialize_embargo(file_set)
              fs_lease   = serialize_lease(file_set)

              file_data = file_set.attributes.merge(
                visibility: file_set.visibility,
                embargo: fs_embargo,
                lease: fs_lease,
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

              # Compact view for file_set
              file_data[:access_effective] = {
                visibility: file_set.visibility,
                under_embargo: fs_embargo ? fs_embargo[:active] : false,
                under_lease: fs_lease ? fs_lease[:active] : false
              }

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

    if digest.respond_to?(:first) && digest.first.present?
      # Expected formats: "urn:sha1:abcdef..." or "sha1:abcdef..."
      original = digest.first.to_s
      parts = original.split(':')
      algorithm = nil
      message = nil

      if parts.length >= 3 && parts[0] == 'urn'
        algorithm = parts[0]      # "urn"
        message   = parts[1]      # "sha1"
      elsif parts.length >= 2
        algorithm = parts[0]      # "sha1"
        message   = parts[1]      # "abcdef..."
      end

      {
        original: digest.first,
        algorithm: algorithm,
        message: message
      }.compact
    else
      {}
    end
  rescue StandardError => e
    puts "Warning: Error extracting checksum for file_set #{file_set.id}: #{e.message}"
    {}
  end
end

# Normalize embargo to a consistent structure for downstream systems
# Falls back to resource-level attributes if the association object is nil
def serialize_embargo(resource)
  emb = resource.try(:embargo)

  # Prefer association fields if present, otherwise read from resource attributes mixed in via EmbargoBehavior
  release = value_presence(
    safe_iso8601(try_send(emb, :embargo_release_date)),
    safe_iso8601(try_send(resource, :embargo_release_date))
  )
  during  = value_presence(
    try_send(emb, :visibility_during_embargo),
    try_send(resource, :visibility_during_embargo)
  )
  after   = value_presence(
    try_send(emb, :visibility_after_embargo),
    try_send(resource, :visibility_after_embargo)
  )
  history = Array(value_presence(
                    try_send(emb, :embargo_history),
                    try_send(resource, :embargo_history)
                  )).compact

  # If we still have nothing meaningful, return nil
  return nil if release.nil? && during.nil? && after.nil? && history.empty?

  now = current_time_utc
  active_date = release && (Time.iso8601(release) > now)
  active_visibility = during && resource.visibility.to_s == during.to_s
  active = !!(active_date && active_visibility)

  {
    id: try_send(emb, :id) || try_send(resource, :embargo_id),
    type: "embargo",
    release_date: release,                 # ISO8601 UTC
    visibility_during: during,             # e.g., "restricted"
    visibility_after: after,               # e.g., "open"
    history: history,                      # textual history
    active: active,
    currently_applied_visibility: !!active_visibility
  }.compact
rescue => e
  puts "Warning: Could not serialize embargo for #{resource.try(:id)}: #{e.message}"
  nil
end

# Normalize lease to a consistent structure for downstream systems
# Falls back to resource-level attributes if the association object is nil
def serialize_lease(resource)
  lease = resource.try(:lease)

  expiration = value_presence(
    safe_iso8601(try_send(lease, :lease_expiration_date)),
    safe_iso8601(try_send(resource, :lease_expiration_date))
  )
  during     = value_presence(
    try_send(lease, :visibility_during_lease),
    try_send(resource, :visibility_during_lease)
  )
  after      = value_presence(
    try_send(lease, :visibility_after_lease),
    try_send(resource, :visibility_after_lease)
  )
  history    = Array(value_presence(
                       try_send(lease, :lease_history),
                       try_send(resource, :lease_history)
                     )).compact

  # If we still have nothing meaningful, return nil
  return nil if expiration.nil? && during.nil? && after.nil? && history.empty?

  now = current_time_utc
  active_date = expiration && (Time.iso8601(expiration) > now)
  active_visibility = during && resource.visibility.to_s == during.to_s
  active = !!(active_date && active_visibility)

  {
    id: try_send(lease, :id) || try_send(resource, :lease_id),
    type: "lease",
    expiration_date: expiration,           # ISO8601 UTC
    visibility_during: during,             # e.g., "open"
    visibility_after: after,               # e.g., "restricted"
    history: history,                      # textual history
    active: active,
    currently_applied_visibility: !!active_visibility
  }.compact
rescue => e
  puts "Warning: Could not serialize lease for #{resource.try(:id)}: #{e.message}"
  nil
end

def value_presence(*vals)
  vals.find { |v| !blank_value?(v) }
end

def blank_value?(v)
  return true if v.nil?
  return v.empty? if v.respond_to?(:empty?)
  false
end

def try_send(obj, meth)
  obj.respond_to?(meth) ? obj.public_send(meth) : nil
end

def safe_iso8601(value)
  return nil if value.nil? || (value.respond_to?(:empty?) && value.empty?)
  t =
    if value.is_a?(Time) || value.is_a?(DateTime)
      value
    elsif value.is_a?(Date)
      value.to_time
    elsif defined?(Time.zone) && Time.zone
      Time.zone.parse(value.to_s)
    else
      Time.parse(value.to_s)
    end
  t&.utc&.iso8601
rescue
  nil
end

def current_time_utc
  (defined?(Time.zone) && Time.zone ? Time.zone.now : Time.now).utc
end

# Define the list of work types
WORK_TYPES = %w[
  AnschutzWork ArchivalMaterial Article Book BookContribution ConferenceItem Dataset DataManagementPlan DenverArticle
  DenverBook DenverBookChapter DenverDataset DenverImage DenverMap DenverMultimedia DenverPresentationMaterial
  DenverSerialPublication DenverThesisDissertationCapstone ExhibitionItem GrantRecord LabNotebook NsuGenericWork
  NsuArticle OpenEducationalResource Report ResearchMethodology Software Minute TimeBasedMedia ThesisOrDissertation
  PacificArticle PacificBook PacificImage PacificThesisDissertation PacificBookChapter PacificMedia PacificNewsClipping
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