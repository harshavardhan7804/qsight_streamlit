import os
import json
import base64
import time
from anthropic import AnthropicVertex
from google.oauth2 import service_account


class SQLScriptGenerator:
    """
    A class to generate SQL transformation queries for data migration.
    """

    def __init__(self):
        """
        Initialize the SQLScriptGenerator class.
        Load required environment variables and create the Anthropic API client.
        """
        self._region = "europe-west1"
        self._project_id = os.getenv("GCP_PROJECT_ID")
        self._credentials = service_account.Credentials.from_service_account_file(
            os.getenv("VERTEX_AI_CREDENTIALS_PATH"),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self._client = self._create_client()
        self._message_history = []

    def _create_client(self):
        """
        Create an instance of the AnthropicVertex client.

        Returns:
            AnthropicVertex: An instance of the AnthropicVertex client.
        """
        return AnthropicVertex(
            region=self._region,
            project_id=self._project_id,
            credentials=self._credentials,
        )

    def _generate_system_prompt(self, legacy_tables):
        """
        Generate a system prompt for SQL query generation.

        Args:
            legacy_tables (dict): Dictionary containing legacy table schemas.
            dependent_tables (list): List of dependent target tables for transformations.

        Returns:
            str: A formatted system prompt string to guide the SQL query generation.
        """
        return f"""
        <Task>  
            You are an advanced SQL assistant specialized in generating SQL transformation queries for migrating data between legacy schemas and target schemas. Given the legacy table schemas, target reference tables(if needed) and a target table schema definition in JSON format, your task is to:  
            1. Analyze the provided schemas.  
            2. Plan the transformation comprehensively, addressing edge cases such as:  
                - Data type mismatches.  
                - Primary and foreign key relationships.  
                - Nullability and default values.  
                - Constraints (e.g., `NOT NULL`, `UNIQUE`, `DEFAULT`) as specified in `alter_commands`.  
                - Naming convention transformations (e.g., camelCase to PascalCase).
            3. Generate only SQL transformation queries tailored to the target schema, ensuring accuracy and optimization.
            4. Do not create any new tables as part of the transformation all tables are already present in the target schema just generate the transformation queries.
            5. Resolve foreign keys using non id columns only and do not use id columns.
        </Task>  

        <Inputs>  
            - Legacy Tables Schemas:  
                {legacy_tables}  
        </Inputs>  

        <Instructions>  
            1. Schema Mapping and Analysis:  
            - Map each target column to its corresponding legacy column(s).  
            - Use JOINs and subqueries to resolve foreign keys and link related tables when necessary.  
            - Dynamically transform data types (e.g., `VARCHAR` to `NVARCHAR`, `BOOLEAN` to `INT`) as required.  
            - Handle auto-generated fields:  
                - Use `NEWID()` for `uniqueidentifier` primary keys.  
                - Apply default values as specified in constraints or `alter_commands`.  
            - Since we have a new id the foreign key columns should be resolved using a non id column with subquery.
            - Handle date fields as follows:  
            - Always use `GETDATE()` for `DateCreated` and `DateLastModified` fields but not for other fields.
            - Do not use legacy date fields for `DateCreated` and `DateLastModified`.
            - Do not use `GETDATE()` for fields other than `DateCreated` and `DateLastModified`.
            - Handle NULL values gracefully in joins and transformations.  
            - Ignore fields with `IDENTITY` in the target schema.  

            2. Transformation Planning:  
            - Detail the step-by-step transformation logic for the target table.  
            - Specify how to map and transform each field.
            - The legacy id and the target id will be different when New ID will be generated so foreign key needs to be resolved using non id columns.
            - Describe how relationships are resolved, including JOINs and subqueries for foreign keys.  
            - Incorporate edge case handling, such as:  
                - Missing legacy fields.  
                - Ambiguous mappings.  
                - Data format conversions (e.g., date or numeric transformations).  

            3. Generate Optimized SQL Queries:  
            - Write accurate SQL queries in T-SQL syntax with clear structure.  
            - Ensure column names are spelled correctly.  
            - Use JOINs, subqueries, and multi-level subqueries for accuracy where needed.  
            - Perform accurate JOINs on correct columns for foreign keys.
            - Resolve foreign key references using meaningful business-related columns instead of IDs, since legacy and target IDs won't match.
            - Use subqueries inside all JOIN conditions to resolve the foreign key dependencies.  
            - Use `CASE` statements wherever required. 
            - Always use `GETDATE()` for `DateCreated` and `DateLastModified` fields but not for other fields.  
            - Do not use `GETDATE()` for fields other than `DateCreated` and `DateLastModified`.
            - Apply `LTRIM()`, `RTRIM()`, and `LOWER()` for column comparisons.  
            - Populate default or derived values for new columns in the target schema.  
            - Include clear comments explaining each transformation step for maintainability.  

            4. Using Subqueries Inside JOINs for Foreign Key Resolutions:
            - Use `LTRIM()`, `RTRIM()`, and `LOWER()` for column comparisons.
            - Use subqueries inside `ON` conditions when directly mapping foreign keys.  
            - Ensure subqueries return the correct data type for JOIN conditions.  
            - If a subquery might return NULL, use LEFT JOIN instead of INNER JOIN.  
            - Avoid unnecessary nested subqueries when a single-level subquery is sufficient.  

            5. Error Handling and Edge Cases:  
            - Address edge cases, such as:  
                - Data type mismatches.  
                - Primary and foreign key relationships.  
                - Nullability and default values.  
                - Constraints (e.g., `NOT NULL`, `UNIQUE`, `DEFAULT`) as specified in `alter_commands`.  
                - Naming convention transformations (e.g., camelCase to PascalCase).  
            - Ensure the generated SQL queries are valid and can be executed without errors.
            - Validate foreign key mappings to avoid orphaned records.  
            - Ensure consistent data migration, even for partially mapped fields.

            6. Recheck the SQL Query:
                - Ensure you haven't missed any field for mapping.
                - The accuracy and precision of the query is very important.

            7. Add detailed comments for JOIN operations to specify:
                    - Which is the legacy table.
                    - Which is the target table.
                    - Which are the target reference tables involved in the transformation.
        </Instructions>  

        <Output Format>
            - Thought:  
                - Summarize how the target table maps to the legacy schema.  
                - Explain challenges or considerations (e.g., mismatched data types, naming conventions, constraints).  

            - Plan:  
                - Detail the steps required to transform data accurately.  
                - Include any necessary joins or transformations.  
                - Specify handling for auto-generated fields, default values, and constraints.  

            - SQL Query:  
                Provide a complete, well-commented SQL query, using the following format:  

                ```sql  
                -- SQL Transformation for [Target Table Name]  
                INSERT INTO [TargetTableName] (  
                    Column1,  
                    Column2,  
                    PrimaryKeyColumn,  
                    ForeignKeyColumn,  
                    AdditionalColumns...,  
                    DateCreated,  
                    DateLastModified  
                )  
                SELECT  
                    NEWID() AS PrimaryKeyColumn, -- Auto-generate uniqueidentifier  
                    LegacyColumn1 AS Column1,  
                    LegacyColumn2 AS Column2,  
                    RelatedTable.ForeignKeyColumn AS ForeignKeyColumn, -- Resolve foreign keys  
                    AdditionalLogic... AS AdditionalColumns, -- Handle derived or default values  
                    GETDATE() AS DateCreated,  
                    GETDATE() AS DateLastModified
                FROM LegacyTable  -- legacy table
                LEFT JOIN RelatedTable  -- target table
                    ON LegacyTable.ForeignKey = RelatedTable.PrimaryKey -- Ensure proper relationships  
                WHERE [Conditions if any];  
                ```  
        </Output Format>

        <Example>
            Input:
                • List of Legacy table schemas: Details about a few relevant tables including dbo.product (Primary Key: [product_type_id] ASC).
                • Target table Schema: Details about dbo.Product table.

            Output:

            - Thought:
            - The target Product table maps primarily to the legacy dbo.products table
            - The legacy id and the target id will be different when New ID will be generated so foreign key needs to be resolved using non id columns.
            - Several foreign key relationships need to be resolved through joins with target reference tables
            - Many default values and constraints need to be handled
            - Several new fields need default values as per alter_commands
            - Need to handle data type conversions (e.g., VARCHAR to NVARCHAR)
            - Need to handle special fields like UsageDesignation based on product type

            - Plan:
            1. Map direct fields from legacy products table
            2. Join with reference tables to resolve foreign keys
            3. Handle default values for new columns
            4. Set appropriate values for required fields
            5. Handle data type conversions
            6. Apply business logic for UsageDesignation based on product type

            ```sql
            -- SQL Transformation for dbo.Product
            INSERT INTO dbo.Product (
                ProductId,
                GlobalProductId,
                ProductTypeId,
                ManufacturerId,
                CatalogNumber,
                CleanCatalogNumber,
                Description,
                DescriptionAlias,
                BrandName,
                QuickReorderNumber,
                LongDescription,
                UnitTypeId,
                Size,
                Latex,
                Stock,
                Discontinued,
                UsageDesignation,
                CostCenter,
                RequireInspection,
                TranslatedTemperatureId,
                ImplantTemperatureTypeId,
                RequireLotNumber,
                RequireExpirationDate,
                RequireLocation,
                RequireRFIDTag,
                ShowFDAAlerts,
                UserId,
                ImagePath,
                IsContrast,
                DateCreated,
                DateLastModified
            )
            SELECT
                NEWID() AS ProductId, -- Generate new uniqueidentifier
                NULL AS GlobalProductId, -- No mapping in legacy schema
                pt.ProductTypeId, -- Resolve using ProductTypeName instead of ID
                m.ManufacturerId, -- Resolve using ManufacturerName instead of ID
                ISNULL(p.catalog_no, '') AS CatalogNumber,
                ISNULL(p.clean_catalog_no, '') AS CleanCatalogNumber,
                p.description AS Description,
                ISNULL(p.nickname, p.description) AS DescriptionAlias, -- Use nickname if available, else description
                ISNULL(p.brand_name, 'N/A') AS BrandName,
                p.quick_reorder_number AS QuickReorderNumber,
                p.long_description AS LongDescription,
                ut.UnitTypeId, -- Resolve using UnitTypeName instead of ID
                p.size AS Size,
                ISNULL(p.latex, 0) AS Latex,
                CASE 
                    WHEN LOWER(p.stock_or_non_stock) = 'stk' THEN 1 
                    ELSE 0 
                END AS Stock,
                ISNULL(p.discontinued, 0) AS Discontinued,
                CASE 
                    WHEN p.detailed_product_type_id IS NOT NULL AND pt.Implant = 1 THEN 'I'
                    ELSE 'S'
                END AS UsageDesignation,
                NULL AS CostCenter, -- No direct mapping
                CASE 
                    WHEN pt.Implant = 1 THEN 1 
                    ELSE 0 
                END AS RequireInspection,
                tt.TranslatedTemperatureId, -- Join with TranslatedTemperature table
                itt.ImplantTemperatureTypeId, -- Join with ImplantTemperatureType table
                1 AS RequireLotNumber, -- Default to true for tracking
                1 AS RequireExpirationDate, -- Default to true for tracking
                1 AS RequireLocation, -- Default to true for tracking
                0 AS RequireRFIDTag, -- Default to false
                CASE 
                    WHEN EXISTS (SELECT 1 FROM dbo.fda_device_match fdm WHERE fdm.product_id = p.product_id) 
                    THEN 1 
                    ELSE 0 
                END AS ShowFDAAlerts,
                u.UserId, -- Join with User table
                NULL AS ImagePath, -- No direct mapping
                0 AS IsContrast, -- Default to false
                GETDATE() AS DateCreated,
                GETDATE() AS DateLaseModified
            FROM dbo.products p -- legacy table
            LEFT JOIN dbo.ProductType pt -- target table
                ON pt.ProductTypeName = (
                    SELECT TOP 1 product_type_name 
                    FROM dbo.product_types 
                    WHERE product_type_id = p.product_type_id
                )
            LEFT JOIN dbo.Manufacturer m -- target table
                ON m.ManufacturerName = (
                    SELECT TOP 1 manufacturer_name 
                    FROM dbo.manufacturers 
                    WHERE manufacturer_id = p.manufacturer_id
                )
            LEFT JOIN dbo.UnitType ut -- target table
                ON ut.UnitTypeName = (
                    SELECT TOP 1 unit_type_name 
                    FROM dbo.unit_types 
                    WHERE unit_type_id = p.unit_type_id
                )
            LEFT JOIN dbo.TranslatedTemperature tt -- target table
                ON tt.TranslatedTemperatureName = (
                    SELECT TOP 1 translated_temperature_name 
                    FROM dbo.translated_temperatures 
                    WHERE translated_temperature_id = p.translated_temperature_id
                )
            LEFT JOIN dbo.ImplantTemperatureType itt -- target table
                ON itt.ImplantTemperatureTypeName = (
                    SELECT TOP 1 implant_temperature_type_name 
                    FROM dbo.implant_temperature_types 
                    WHERE implant_temperature_type_id = p.implant_temperature_type_id
                )
            LEFT JOIN dbo.[User] u -- target table
                ON u.EmployeNumber = (
                    SELECT TOP 1 employee_number 
                    FROM dbo.users 
                    WHERE user_id = p.user_id
                )
            WHERE p.deleted = 0 -- Only include non-deleted products;
            ```

            This transformation query:
            1. Handles all required field mappings from legacy to target schema
            2. Resolves foreign key relationships through appropriate joins
            3. Sets default values as per alter_commands
            4. Handles data type conversions
            5. Implements business logic for fields like UsageDesignation
            6. Includes proper NULL handling
            7. Sets appropriate values for tracking fields
            8. Uses GETDATE() for DateLaseModified
            9. Preserves legacy DateCreated when available
            10. Excludes deleted records from the legacy system

            The query uses LEFT JOINs to ensure no data is lost due to missing foreign key references and includes subqueries in the JOIN conditions to properly resolve the relationships between legacy and target tables
        </Example>

        Generate SQL transformation queries for every target schema with this level of precision.  
        """

    def _calculate_token_cost(
        self,
        input_tokens,
        output_tokens,
        input_token_cost_per_million=3,
        output_token_cost_per_million=15,
    ):
        """
        Calculate the cost of input and output tokens based on the given rates.

        Args:
            input_tokens (int): Number of input tokens.
            output_tokens (int): Number of output tokens.
            input_token_cost_per_million (float): Cost per million input tokens (default: $3).
            output_token_cost_per_million (float): Cost per million output tokens (default: $15).

        Returns:
            dict: A dictionary containing input tokens, output tokens, and costs.
        """
        try:
            # Validate inputs
            if not isinstance(input_tokens, int) or not isinstance(output_tokens, int):
                raise ValueError(
                    "Both input_tokens and output_tokens must be integers."
                )

            if input_tokens < 0 or output_tokens < 0:
                raise ValueError("Token counts cannot be negative.")

            # Convert per million token cost to cost per token
            input_token_cost = input_token_cost_per_million / 1_000_000
            output_token_cost = output_token_cost_per_million / 1_000_000

            # Calculate costs
            total_input_cost = input_tokens * input_token_cost
            total_output_cost = output_tokens * output_token_cost
            total_cost = total_input_cost + total_output_cost

            # Return the results in a dictionary
            return {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "input_cost": round(total_input_cost, 7),
                "output_cost": round(total_output_cost, 7),
                "total_cost": round(total_cost, 7),
            }
        except Exception as e:
            return {"error": str(e)}

    def _call_claude(self, legacy_tables):
        """
        Make a call to the Anthropic API to generate an SQL query for data migration.

        Args:
            messages (list): List of messages for Claude API.
            legacy_tables (dict): Dictionary containing legacy table schemas.
            dependent_tables (list): List of dependent target tables for transformations.

        Returns:
            tuple: A tuple containing the generated SQL query (str) and token usage information (dict).
        """
        try:
            system_prompt = self._generate_system_prompt(legacy_tables)
            message = self._client.messages.create(
                system=system_prompt,
                model="claude-3-5-sonnet-v2@20241022",
                max_tokens=8192,
                temperature=0,
                messages=self._message_history,
            )
            response = json.loads(message.model_dump_json(indent=2))
            input_tokens = response.get("usage", {}).get("input_tokens", 0)
            output_tokens = response.get("usage", {}).get("output_tokens", 0)
            usage_info = self._calculate_token_cost(input_tokens, output_tokens)
            return (response["content"][0]["text"], usage_info)
        except Exception as e:
            print(f"An error occurred during Anthropic API call: {e}")
            return None

    def get_sql_query(self, legacy_tables, target_schema, dependent_tables):
        """
        Generate an SQL query to migrate data from legacy tables to the target schema.

        This method uses the Claude API to generate SQL transformation queries,
        leveraging both the legacy table schemas and dependent target tables.
        It appends a message to the history, makes an API call, and returns the response along with usage information.

        Args:
            legacy_tables (dict): Schema definitions of the legacy tables.
            target_schema (dict): Schema definition of the target table.
            dependent_tables (list): List of dependent target tables for transformations.

        Returns:
            tuple: The generated SQL query (str) and token usage information (dict).
        """
        self._message_history.append(
            {
                "role": "user",
                "content": f"Write a transformation query for the given Target Schema based on the Legacy tables:\nTarget Schema:{target_schema}\n\nThese are the target reference tables for the target schema:\n{dependent_tables}",
            }
        )
        start_time = time.perf_counter()
        response, usage_info = self._call_claude(legacy_tables)
        end_time = time.perf_counter()
        print(f"Execution Time in ClaudeAPI: {end_time - start_time:.2f} " f"second")
        return (response, usage_info)
        
if __name__ == "__main__":
    sql_writer = SQLScriptGenerator()
    response, usage_info = sql_writer.get_sql_query(
        legacy_tables="legacy_tables",
        target_schema="table",
        dependent_tables="dependent_tables",
    )
    print(response)
    print(usage_info)