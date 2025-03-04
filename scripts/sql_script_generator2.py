import requests
import os
import json
import time

class SQLScriptGenerator:
    """
    A class to generate SQL transformation queries for data migration.
    """
    def __init__(self):
        self._message_history = []

    def _send_payload_to_api(self, prompt, system):

        payload = {
            "conciergeId": "7f5c74a3-409f-405a-86da-9431c9d29c27",
            "conciergeName": "docaidatamigration",
            "organizationId": "d73b4e26-10f0-4f57-8b11-5a6e33c632b1",
            "organizationName": "techolution",
            "guestId": "techolution-docaidatamigration-738fb1a3-fc48-4c64-bf1e-079c9cca7145",
            "userId": "5b6b4184-98f3-40b3-acfa-85cc5780e153",
            "userName": "docai_studio",
            "assistant_type": "normal",
            "question": prompt,
            "prompt": system,
            "referenceDocsCount": 0,
            "proposals_file": "",
            "proposals_section": "",
            "proposals_template": "",
            "images": [],
            "model_names": {"google": "claude-3.5-sonnet"},
            "isStreamResponseOn": False,
            "is_generative": False,
            "isAgentsOn": True,
            "confidenceScoreThreshold": 70,
            "chatHistory": self._message_history,
            "aiChatHistory": [],
            "modelType": "google",
            "pinecone_index": "techolution-docaidatamigration",
            "databaseType": "alloydb",
            "database_index": "techolution-docaidatamigration",
            "isCoPilotOn": False,
            "slack_webhook_url": "",
            "requestId": "requestId-3981cb2f-952c-4dd3-b408-830e3c886432",
            "chatLowConfidenceMessage": "This request seems to be out of what I am allowed to answer. Kindly check with the technical team - ellm-studio@techolution.com. Thank you!",
            "autoai": "679b998a99a3e513ddb064a8",
            "documentRetrieval": "679b998999a3e513ddb0648d",
            "answerEvaluation": "679b998999a3e513ddb06496",
            "bestAnswer": "679b998a99a3e513ddb0649f",
            "metadata": '{"userName":"docai_studio","userEmailId":"docaistudio@techolution.com","llm":"claude-3.5-sonnet"}',
            "source": "",
            "target": "",
            "evaluationCriteria": {},
            "include_link": False,
            "isInternetSearchOn": False,
            "intermediate_model": "gpt-4-32k",
            "isSpt": False,
            "sptProject": "acs",
            "numberOfCitations": 0,
            "sptNote": "Note: The current response is below the confidence threshold. Please use the information carefully.",
            "wordsToReplace": {},
            "number_of_next_query_suggestions": 0,
            "agents": [],
            "isSelfharmOn": False,
            "selfharmDefaultResponse": "",
            "multiAgentToggle": False,
            "useAgent": {},
            "isPlanBeforeOrchestrator": False,
            "isDocumentRetrieval": False,
            "isConsultativeMode": False,
            "chatSessionId": "techolution-docaidatamigration-3216b19a-9bd7-4656-bf6e-d5fed2969e9b",
            "agentSettings": {"orchestratorPrompt": system},
            "rootOrchestrator": {"dbConfig": {}},
            "isRESTrequest": False,
        }
        url = "https://dev-egpt.techo.camp/predict"
        # url = "https://wd2hqk6b-5001.inc1.devtunnels.ms/predict"
        try:
            # Send a POST request to the API with the payload
            response = requests.post(url, json=payload)
            # Check if the request was successful (status code 200)

            if response.status_code == 200:
                # Return the response from the API
                return response.json()["Answer"]
            else:
                # Print an error message if the request was not successful
                print(f"Error: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            # Print an error message if there was an exception
            print(f"An error occurred: {e}")
            return None

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
        prompt = f"Write a transformation query for the given Target Schema based on the Legacy tables:\nTarget Schema:{target_schema}\n\nThese are the target reference tables for the target schema:\n{dependent_tables}"
        self._message_history.append(
            {
                "role": "user",
                "content": prompt,
            }
        )
        system_prompt = self._generate_system_prompt(legacy_tables=legacy_tables)
        start_time = time.perf_counter()
        response = self._send_payload_to_api(prompt=prompt, system=system_prompt) 
        end_time = time.perf_counter()
        print(f"Execution Time in ELLM_API: {end_time - start_time:.2f} " f"second")
        self._message_history.append(
            {
                "role": "assistant",
                "content": response,
            }
        )
        return response

if __name__ == "__main__":
    sql_script_generator = SQLScriptGenerator()
    sql_script_generator.get_sql_query()
