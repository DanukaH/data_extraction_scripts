# Define a method to extract access control data for a specific tenant
def extract_access_controls_for_tenant(tenant_name)
  puts "Extracting access control data for tenant: #{tenant_name}"

  begin
    # Switch to the tenant's schema
    Apartment::Tenant.switch!(tenant_name)

    # Open a file to write the extracted data
    File.open("#{tenant_name}_access_controls.json", 'w') do |file|
      # Write the opening bracket of a JSON array
      file.puts "["

      first_entry = true # Track whether it's the first entry for JSON formatting

      # Iterate through all access control records in the tenant
      Hydra::AccessControl.find_each do |access_control|
        # Use attributes method to fetch database data for the record
        access_control_data = access_control.attributes

        # Write the JSON representation of the access control data to the file
        file.puts (first_entry ? "" : ",") + access_control_data.to_json
        first_entry = false
      end

      # Write the closing bracket of the JSON array
      file.puts "]"
    end

    puts "Finished extracting access control data for tenant: #{tenant_name}"
  rescue StandardError => e
    # Log any unexpected errors for the tenant
    puts "Error processing access controls for tenant #{tenant_name}: #{e.message}"
  ensure
    # Reset to the default tenant to free up memory
    Apartment::Tenant.reset

    # Trigger garbage collection to reduce memory usage
    GC.start
  end
end

# Main execution logic
if ARGV.length != 1
  puts "Usage: ruby extract_access_controls.rb <tenant_cname>"
  exit 1
end

tenant_name = ARGV[0] # Get the tenant cname from command-line arguments

puts "Starting access control data extraction for tenant: #{tenant_name}"

extract_access_controls_for_tenant(tenant_name)

puts "Completed access control data extraction for tenant: #{tenant_name}"
puts "===================================================================="