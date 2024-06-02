# Class validate data
class ValidateData:

    def __init__(self):
        pass

    # Check column match from dataframe
    def check_schema(self, validator, column_check_match):
        """
        Validate for schema

        Args:
        - validator: Instance for great expectatios
        - column: Column check match

        Returns:
        - String value for schema check
        """

        # Validator
        validator.expect_table_columns_to_match_set(column_check_match, exact_match = True)
        
        # Get validator result
        validation_results = validator.validate()

        # List for append value of missing columns
        missing_columns_list = []

        # Iterating through the results
        for result in validation_results['results']:
            # Checking if the expectation failed
            if not result['success']:
                # Extracting information about mismatched and missing columns
                mismatched_info = result['result']['details']['mismatched']
                unexpected_columns = mismatched_info['unexpected']
                missing_columns = mismatched_info['missing']

                # Concat and add to list
                missing_info = f"Missed column: {missing_columns}, Unexpected column: {unexpected_columns}"
                missing_columns_list.append(missing_info)

        # Convert to string for list
        missing_column_str = '\n'.join(missing_columns_list)

        return missing_column_str 


    # Check null from dataframe
    def check_null(self, validator, colum_check_null):
        """
        Validate for null value

        Args:
        - validator: Instance for great expectatios
        - column: Column check null

        Returns:
        - String value for null column and count
        """

        # expect the following column values to not be null (perhaps these are needed for downstream analytics)
        validator.expect_column_values_to_not_be_null(column = colum_check_null)

        # Run the validator
        validation_results = validator.validate()

        # List
        column_null_list = []

        for result in validation_results['results']:
            # Checking if the expectation failed
            if not result['success']:
                # Extracting information about null values
                null_info = result['result']
                element_count = null_info['element_count']
                unexpected_count = null_info['unexpected_count']

                # Get value and add to list
                column_null_info = f"Column: {colum_check_null}, Number of record: {element_count}, Number of null: {unexpected_count}"
                column_null_list.append(column_null_info)

        # Convert to strings
        null_column_str = '\n'.join(column_null_list)

        return null_column_str
