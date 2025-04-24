# Define a method to extract roles and additional fields for a specific tenant
def extract_roles_for_tenant(tenant_name)
  puts "Extracting roles for tenant: #{tenant_name}"

  begin
    # Switch to the tenant's schema
    Apartment::Tenant.switch!(tenant_name)

    # Open a file to write the extracted data
    File.open("#{tenant_name}_roles.json", 'w') do |file|
      # Write the opening bracket of a JSON array
      file.puts "["

      first_entry = true # Track if it's the first role for proper JSON formatting

      # Iterate through all roles in the tenant
      Role.find_each do |role|
        # Collect role information with additional fields
        role_data = {
          id: role.id,
          name: role.name,
          resource_type: role.resource_type,
          resource_id: role.resource_id,
          created_at: role.created_at,
          updated_at: role.updated_at
        }

        # Write the JSON representation of the role data to the file
        file.puts (first_entry ? "" : ",") + role_data.to_json
        first_entry = false
      end

      # Write the closing bracket of the JSON array
      file.puts "]"
    end

    puts "Finished extracting roles for tenant: #{tenant_name}"
  rescue StandardError => e
    # Log any unexpected errors for the tenant
    puts "Error processing roles for tenant #{tenant_name}: #{e.message}"
  ensure
    # Reset to the default tenant to free up memory
    Apartment::Tenant.reset

    # Trigger garbage collection to reduce memory usage
    GC.start
  end
end

# Main execution logic
if ARGV.length != 1
  puts "Usage: ruby extract_roles.rb <tenant_cname>"
  exit 1
end

tenant_name = ARGV[0] # Get the tenant cname from command-line arguments

puts "Starting roles extraction for tenant: #{tenant_name}"

extract_roles_for_tenant(tenant_name)

puts "Completed roles extraction for tenant: #{tenant_name}"
puts "===================================================================="