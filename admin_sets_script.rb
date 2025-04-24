# Define a method to extract admin set data for a specific tenant
def extract_admin_sets_for_tenant(tenant_name)
  puts "Extracting admin set data for tenant: #{tenant_name}"

  begin
    # Switch to the tenant's schema
    Apartment::Tenant.switch!(tenant_name)

    # Open a file to write the extracted data
    File.open("#{tenant_name}_admin_sets.json", 'w') do |file|
      # Write the opening bracket of a JSON array
      file.puts "["

      first_entry = true # Track whether it's the first entry for JSON formatting

      # Iterate through all admin sets in the tenant
      AdminSet.find_each do |admin_set|
        # Use attributes method to fetch database data for the record
        admin_set_data = admin_set.attributes

        # Write the JSON representation of the admin set data to the file
        file.puts (first_entry ? "" : ",") + admin_set_data.to_json
        first_entry = false
      end

      # Write the closing bracket of the JSON array
      file.puts "]"
    end

    puts "Finished extracting admin set data for tenant: #{tenant_name}"
  rescue StandardError => e
    # Log any unexpected errors for the tenant
    puts "Error processing admin sets for tenant #{tenant_name}: #{e.message}"
  ensure
    # Reset to the default tenant to free up memory
    Apartment::Tenant.reset

    # Trigger garbage collection to reduce memory usage
    GC.start
  end
end

# Main execution logic
if ARGV.length != 1
  puts "Usage: ruby extract_admin_sets.rb <tenant_cname>"
  exit 1
end

tenant_name = ARGV[0] # Get the tenant cname from command-line arguments

puts "Starting admin set data extraction for tenant: #{tenant_name}"

extract_admin_sets_for_tenant(tenant_name)

puts "Completed admin set data extraction for tenant: #{tenant_name}"
puts "===================================================================="