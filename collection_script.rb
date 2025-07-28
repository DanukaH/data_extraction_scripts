def extract_collection_data(tenant_cname)
  tenant = Account.find_by(cname: tenant_cname)

  if tenant.nil?
    puts "Error: No tenant found with cname: #{tenant_cname}"
    return
  end

  tenant_name = tenant.tenant
  puts "Extracting collection data for tenant: #{tenant_name} (cname: #{tenant_cname})"

  begin
    Apartment::Tenant.switch!(tenant_name)

    # Query Solr directly for collections
    solr_collections = ActiveFedora::SolrService.query(
      "has_model_ssim:Collection",
      rows: 1000000,
      fl: '*'
    )

    puts "Found #{solr_collections.size} collections"

    File.open("#{tenant_cname}_collections_data.json", 'w') do |file|
      file.puts "["
      first_entry = true

      solr_collections.each_with_index do |solr_doc, index|
        begin
          puts "Processing collection #{index + 1}/#{solr_collections.size}: #{solr_doc['id']}"

          collection_data = {
            'id' => solr_doc['id'],
            'depositor' => solr_doc['depositor_ssim']&.first,
            'title' => solr_doc['title_tesim'] || [],
            'date_uploaded' => nil,
            'date_modified' => nil,
            'head' => [],
            'tail' => [],
            'collection_type_gid' => solr_doc['collection_type_gid_ssim']&.first,
            'label' => nil,
            'relative_path' => nil,
            'import_url' => nil,
            'resource_type' => solr_doc['resource_type_tesim'] || [],
            'creator' => solr_doc['creator_tesim'] || [],
            'contributor' => solr_doc['contributor_tesim'] || [],
            'description' => solr_doc['description_tesim'] || [],
            'keyword' => solr_doc['keyword_tesim'] || [],
            'license' => solr_doc['license_tesim'] || [],
            'rights_statement' => solr_doc['rights_statement_tesim'] || [],
            'publisher' => solr_doc['publisher_tesim'] || [],
            'date_created' => solr_doc['date_created_tesim'] || [],
            'subject' => solr_doc['subject_tesim'] || [],
            'language' => solr_doc['language_tesim'] || [],
            'identifier' => solr_doc['identifier_tesim'] || [],
            'based_near' => solr_doc['based_near_tesim'] || [],
            'related_url' => solr_doc['related_url_tesim'] || [],
            'bibliographic_citation' => solr_doc['bibliographic_citation_tesim'] || [],
            'source' => solr_doc['source_tesim'] || [],
            'representative_id' => nil,
            'thumbnail_id' => nil,
            'visibility' => solr_doc['visibility_ssi'],
            'collection_logo_local_path' => nil
          }

          # Set up access control structure
          collection_data['access_control'] = {
            'id' => solr_doc['access_control_id_ssi'],
            'permissions' => []
          }

          # Add permissions based on edit access
          if solr_doc['edit_access_person_ssim'].present?
            solr_doc['edit_access_person_ssim'].each do |user|
              collection_data['access_control']['permissions'] << {
                'id' => "#{solr_doc['access_control_id_ssi']}/#{SecureRandom.hex(16)}",
                'mode' => [{ 'id' => 'http://www.w3.org/ns/auth/acl#Write' }],
                'agent' => [{ 'id' => "http://projecthydra.org/ns/auth/person##{user}" }],
                'access_to_id' => solr_doc['id']
              }
            end
          end

          if solr_doc['edit_access_group_ssim'].present?
            solr_doc['edit_access_group_ssim'].each do |group|
              collection_data['access_control']['permissions'] << {
                'id' => "#{solr_doc['access_control_id_ssi']}/#{SecureRandom.hex(16)}",
                'mode' => [{ 'id' => 'http://www.w3.org/ns/auth/acl#Write' }],
                'agent' => [{ 'id' => "http://projecthydra.org/ns/auth/group##{group}" }],
                'access_to_id' => solr_doc['id']
              }
            end
          end

          # Get works in the collection using Solr
          member_works = ActiveFedora::SolrService.query(
            "member_of_collection_ids_ssim:#{solr_doc['id']}",
            rows: 1000000,
            fl: '*'
          )

          works_data = []

          member_works.each do |work_doc|
            work_data = {
              'id' => work_doc['id'],
              'title' => work_doc['title_tesim']&.first,
              'work_type' => work_doc['has_model_ssim']&.first,
              'visibility' => work_doc['visibility_ssi'],
              'date_uploaded' => work_doc['system_create_dtsi'],
              'date_modified' => work_doc['system_modified_dtsi'],
              'creator' => work_doc['creator_tesim'],
              'description' => work_doc['description_tesim'],
              'keyword' => work_doc['keyword_tesim'],
              'rights_statement' => work_doc['rights_statement_tesim'],
              'subject' => work_doc['subject_tesim'],
              'publisher' => work_doc['publisher_tesim'],
              'language' => work_doc['language_tesim'],
              'identifier' => work_doc['identifier_tesim'],
              'resource_type' => work_doc['resource_type_tesim'],
              'file_set_ids' => work_doc['file_set_ids_ssim']
            }

            # Get file sets for this work using Solr
            if work_doc['file_set_ids_ssim'].present?
              file_sets = ActiveFedora::SolrService.query(
                "id:(#{work_doc['file_set_ids_ssim'].join(' OR ')})",
                rows: 1000000,
                fl: '*'
              )

              work_data['files'] = file_sets.map do |fs_doc|
                {
                  'id' => fs_doc['id'],
                  'title' => fs_doc['label_tesim']&.first || fs_doc['title_tesim']&.first,
                  'filename' => fs_doc['label_tesim']&.first,
                  'mime_type' => fs_doc['mime_type_ssi'],
                  'size' => fs_doc['file_size_lts'],
                  'date_uploaded' => fs_doc['system_create_dtsi'],
                  'visibility' => fs_doc['visibility_ssi']
                }
              end
              work_data['file_count'] = work_data['files'].size
            end

            works_data << work_data
          end

          collection_data['works'] = works_data
          collection_data['work_count'] = works_data.size

          # Write to file with proper JSON formatting
          json_output = (first_entry ? "" : ",") + JSON.pretty_generate(collection_data)
          file.puts json_output
          first_entry = false

          if (index + 1) % 10 == 0
            puts "Processed #{index + 1} collections..."
            GC.start
          end

        rescue StandardError => e
          puts "Error processing collection #{solr_doc['id']}: #{e.message}"
          error_data = {
            'id' => solr_doc['id'],
            'error' => "Failed to process collection: #{e.message}"
          }
          file.puts (first_entry ? "" : ",") + JSON.pretty_generate(error_data)
          first_entry = false
        end
      end

      file.puts "]"
    end

    puts "Successfully completed extraction for tenant: #{tenant_name}"

  rescue StandardError => e
    puts "Error processing collection data for tenant #{tenant_name}: #{e.message}"
    puts "Backtrace:"
    puts e.backtrace
  ensure
    Apartment::Tenant.reset
    GC.start
  end
end

# Main execution
if ARGV.length != 1
  puts "Usage: ruby extract_collection_data.rb <tenant_cname>"
  exit 1
end

tenant_cname = ARGV[0]

puts "==========================================="
puts "Starting collection data extraction"
puts "Tenant CNAME: #{tenant_cname}"
puts "Time: #{Time.now}"
puts "==========================================="

extract_collection_data(tenant_cname)

puts "==========================================="
puts "Completed collection data extraction"
puts "Time: #{Time.now}"
puts "==========================================="