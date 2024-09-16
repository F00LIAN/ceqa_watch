from data_processor import CEQADataProcessor

if __name__ == "__main__":
    
    # Example list of cities to process
    cities = ["Lancaster, City of", "Los Angeles, City of", "San Diego, City of"]

    # Initialize the CEQADataProcessor with the table name
    processor = CEQADataProcessor(table_name="ceqa_data")
    
    # Run the data processor for the specified cities
    processor.run_for_cities(cities)