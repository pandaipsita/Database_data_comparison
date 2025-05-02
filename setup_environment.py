import os
import yaml
import great_expectations as ge
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig


def setup_environment():
    # Create default config if it doesn't exist
    if not os.path.exists("config.yaml"):
        config = {
            "chromadb_path": "./chromadb",
            "data_directory": "./data_validation/",
            "ge_dir": "./great_expectations",
            "embedding_model": "nomic-embed-text",
            "similarity_threshold": 0.85,
            "max_results": 10,
            "schemas": [
                "employee_management",
                "contractor_management"
            ]
        }

        with open("config.yaml", "w") as f:
            yaml.dump(config, f, default_flow_style=False)

        print("✅ Created default config.yaml")
    else:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)

    # Ensure directories exist
    os.makedirs(config["data_directory"], exist_ok=True)
    os.makedirs(config["chromadb_path"], exist_ok=True)
    os.makedirs("validation_reports", exist_ok=True)

    # Set up Great Expectations directory
    ge_dir = config["ge_dir"]
    os.makedirs(ge_dir, exist_ok=True)

    # Check if Great Expectations is already initialized
    if not os.path.exists(os.path.join(ge_dir, "great_expectations.yml")):
        print("Initializing Great Expectations...")

        try:
            # Use the GreatExpectationsDataContext API
            context_config = DataContextConfig(
                config_version=3,
                plugins_directory=os.path.join(ge_dir, "plugins"),
                expectations_store_name="expectations_store",
                validations_store_name="validations_store",
                evaluation_parameter_store_name="evaluation_parameter_store",
                stores={
                    "expectations_store": {"class_name": "ExpectationsStore",
                                           "store_backend": {"class_name": "TupleFilesystemStoreBackend",
                                                             "base_directory": os.path.join(ge_dir, "expectations")}},
                    "validations_store": {"class_name": "ValidationsStore",
                                          "store_backend": {"class_name": "TupleFilesystemStoreBackend",
                                                            "base_directory": os.path.join(ge_dir, "validations")}},
                    "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"}
                },
                data_docs_sites={
                    "local_site": {
                        "class_name": "SiteBuilder",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": os.path.join(ge_dir, "data_docs")
                        }
                    }
                }
            )

            # Create context manually
            context = BaseDataContext(
                project_config=context_config,
                context_root_dir=ge_dir
            )

            # Save the configuration
            context.save_config_variable("ge_dir", ge_dir)

            print("✅ Great Expectations initialized successfully")

        except Exception as e:
            print(f"Error initializing Great Expectations: {e}")
            print("Continuing without Great Expectations...")
    else:
        print("✅ Great Expectations already initialized")

    print("✅ Environment setup complete")


if __name__ == "__main__":
    setup_environment()