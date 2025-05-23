def extract_user_data(tenant_cname)
  # Find the tenant by cname
  tenant = Account.find_by(cname: tenant_cname)

  if tenant.nil?
    puts "Error: No tenant found with cname: #{tenant_cname}"
    return
  end

  tenant_name = tenant.tenant # Get the actual tenant name from the account

  puts "Extracting user account data for tenant: #{tenant_name} (cname: #{tenant_cname})"

  begin
    # Switch to the tenant's schema using the actual tenant name
    Apartment::Tenant.switch!(tenant_name)

    # Open a file to write the extracted user data
    File.open("#{tenant_cname}_users_data.json", 'w') do |file|
    # Write the opening bracket of a JSON array
      file.puts "["

      first_entry = true # Track if it's the first user for proper JSON formatting

      # Iterate through all user accounts in the tenant
      User.find_each do |user|
        # Extract user attributes and merge relevant data
        user_data = user.attributes.except('encrypted_password', 'reset_password_token', 'remember_created_at')

        # Add user roles and other associated data (if any)
        user_data[:roles] = user.roles.pluck(:name) if user.respond_to?(:roles)

        # Write the JSON representation of the user data to the file
        file.puts (first_entry ? "" : ",") + user_data.to_json
        first_entry = false
      end

      # Write the closing bracket of the JSON array
      file.puts "]"
    end

    puts "Finished extracting user data for tenant: #{tenant_name}"
  rescue StandardError => e
    # Log any unexpected errors for the tenant
    puts "Error processing user data for tenant #{tenant_name}: #{e.message}"
  ensure
    # Reset to the default tenant to free up memory
    Apartment::Tenant.reset

    # Trigger garbage collection to reduce memory usage
    GC.start
  end
end

# Main execution logic
if ARGV.length != 1
  puts "Usage: ruby extract_user_data.rb <tenant_cname>"
  exit 1
end

tenant_cname = ARGV[0] # Get the tenant cname from command-line arguments

puts "Starting user data extraction for tenant cname: #{tenant_cname}"

extract_user_data(tenant_cname)

puts "Completed user data extraction for tenant: #{tenant_cname}"
puts "==========================================="