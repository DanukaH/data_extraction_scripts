# Define a method to extract collection data for a specific tenant
def extract_collection_data(tenant_name)
  puts "Extracting collection data for tenant: #{tenant_name}"

  begin
    # Switch to the tenant's schema
    Apartment::Tenant.switch!(tenant_name)

    # Open a file to write the extracted collection data
    File.open("#{tenant_name}_collections_data.json", 'w') do |file|
      # Write the opening bracket of a JSON array
      file.puts "["

      first_entry = true # Track if it's the first collection for proper JSON formatting

      # Iterate through all collections in the tenant
      Collection.find_each do |collection|
        # Extract collection metadata and associated works
        collection_data = collection.attributes

        # If the collection includes associated works (e.g., members), retrieve them
        works_data = collection.members.map do |work|
          work.attributes.merge(
            work_title: work.title.first, # Example: retrieving the title of the work
            work_type: work.class.name
          )
        end

        # Add works data to the collection data
        collection_data[:works] = works_data

        # Write the JSON representation of the collection data to the file
        file.puts (first_entry ? "" : ",") + collection_data.to_json
        first_entry = false
      end

      # Write the closing bracket of the JSON array
      file.puts "]"
    end

    puts "Finished extracting collection data for tenant: #{tenant_name}"
  rescue StandardError => e
    # Log any unexpected errors for the tenant
    puts "Error processing collection data for tenant #{tenant_name}: #{e.message}"
  ensure
    # Reset to the default tenant to free up memory
    Apartment::Tenant.reset

    # Trigger garbage collection to reduce memory usage
    GC.start
  end
end

# Main execution logic
if ARGV.length != 1
  puts "Usage: ruby extract_collection_data.rb <tenant_cname>"
  exit 1
end

tenant_name = ARGV[0] # Get the tenant cname from command-line arguments

puts "Starting collection data extraction for tenant: #{tenant_name}"

extract_collection_data(tenant_name)

puts "Completed collection data extraction for tenant: #{tenant_name}"
puts "==============================================="