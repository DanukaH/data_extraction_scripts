# Define a method to extract user roles for a specific tenant
def extract_user_roles(tenant_name)
  puts "Extracting user roles for tenant: #{tenant_name}"

  begin
    # Switch to the tenant's schema
    Apartment::Tenant.switch!(tenant_name)

    # Open a file to write the extracted user role data
    File.open("#{tenant_name}_user_roles.json", 'w') do |file|
      # Write the opening bracket of a JSON array
      file.puts "["

      first_entry = true # Track if it's the first user for proper JSON formatting

      # Iterate through all user accounts in the tenant
      User.find_each do |user|
        # Extract user roles (if the roles association is present)
        user_roles = user.roles.pluck(:name) if user.respond_to?(:roles)

        # Create a hash with user id, email, and their roles
        role_data = {
          user_id: user.id,
          email: user.email,
          roles: user_roles || [] # Default to an empty array if no roles
        }

        # Write the JSON representation of the role data to the file
        file.puts (first_entry ? "" : ",") + role_data.to_json
        first_entry = false
      end

      # Write the closing bracket of the JSON array
      file.puts "]"
    end

    puts "Finished extracting user roles for tenant: #{tenant_name}"
  rescue StandardError => e
    # Log any unexpected errors for the tenant
    puts "Error processing user roles for tenant #{tenant_name}: #{e.message}"
  ensure
    # Reset to the default tenant to free up memory
    Apartment::Tenant.reset

    # Trigger garbage collection to reduce memory usage
    GC.start
  end
end

# Main execution logic
if ARGV.length != 1
  puts "Usage: ruby extract_user_roles.rb <tenant_cname>"
  exit 1
end

tenant_name = ARGV[0] # Get the tenant cname from command-line arguments

puts "Starting user role extraction for tenant: #{tenant_name}"

extract_user_roles(tenant_name)

puts "Completed user role extraction for tenant: #{tenant_name}"
puts "==========================================="