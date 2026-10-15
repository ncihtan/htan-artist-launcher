#!/usr/bin/env python3
"""
HTAN Google Cloud Storage Staging Script

This script filters samplesheets for Google Cloud Storage files, uploads them to S3 staging,
and launches the nf-synapse pipeline to stage the data for further processing.
"""

import argparse
import logging
import re
import string
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import shortuuid
from dotenv import load_dotenv
from seqerakit import seqeraplatform

# Load environment variables
load_dotenv()

# Constants
DEFAULT_WORKSPACE = "253119656982040"  # htan-project
DEFAULT_COMPUTE_ENV = "3k6bQPIqpII2nGlDMbc9Mv"  # htan-project-spot-v12
DEFAULT_PIPELINE_URL = "https://github.com/Sage-Bionetworks-Workflows/nf-synapse"
DEFAULT_SAMPLESHEET = "samplesheet/artist-samplesheet.csv"
DEFAULT_STAGING_BUCKET = "s3://htan-project-tower-scratch/work/"
DEFAULT_OUTPUT_BUCKET = "s3://htan-project-tower-scratch/work/"

# Initialize short UUID generator
alphabet = string.ascii_lowercase + string.ascii_uppercase + string.digits
shortuuid_generator = shortuuid.ShortUUID(alphabet=alphabet)


def setup_logging(debug: bool = False) -> None:
    """Set up logging configuration."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def generate_run_id() -> str:
    """Generate a unique 4-character identifier for the run."""
    return shortuuid_generator.random(length=4)


def validate_samplesheet(samplesheet_path: Path) -> None:
    """Validate that the samplesheet exists and has required columns."""
    if not samplesheet_path.exists():
        raise FileNotFoundError(f"Samplesheet not found: {samplesheet_path}")

    try:
        df = pd.read_csv(samplesheet_path)
        required_columns = ["id", "image", "cloud_provider"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in samplesheet: {missing_columns}"
            )

        if df.empty:
            raise ValueError("Samplesheet is empty")

        logging.info(f"Validated samplesheet with {len(df)} rows")
    except Exception as e:
        raise ValueError(f"Invalid samplesheet format: {e}")


def filter_gs_files(samplesheet_path: Path, output_dir: Path) -> Path:
    """Filter samplesheet for Google Cloud Storage (gs://) files only."""
    logging.info("Filtering samplesheet for Google Cloud Storage files")

    df = pd.read_csv(samplesheet_path)

    # Filter for gs cloud provider
    gs_df = df[df["cloud_provider"] == "gs"].copy()

    if gs_df.empty:
        raise ValueError("No Google Cloud Storage files found in samplesheet")

    logging.info(
        f"Found {len(gs_df)} Google Cloud Storage files out of {len(df)} total"
    )

    # Copy the original image column to image_gs
    gs_df["image_gs"] = gs_df["image"].copy()

    # Transform the image column to synapse URI format using the id column
    def format_synapse_uri(syn_id):
        """Format synapse ID to syn:// URI format."""
        if pd.isna(syn_id):
            return syn_id
        syn_id_str = str(syn_id)
        if syn_id_str.startswith("syn://"):
            return syn_id_str
        elif syn_id_str.startswith("syn"):
            return f"syn://{syn_id_str}"
        else:
            logging.warning(
                f"Unexpected ID format: {syn_id_str}, adding syn:// prefix anyway"
            )
            return f"syn://{syn_id_str}"

    # Replace image column with formatted synapse URIs from id column
    gs_df["image"] = gs_df["id"].apply(format_synapse_uri)

    # Log some examples of the transformation
    sample_rows = gs_df[["id", "image", "image_gs"]].head(3)
    logging.info(f"Example transformations:")
    for _, row in sample_rows.iterrows():
        logging.info(
            f"  ID: {row['id']} -> Image: {row['image']} (GS backup: {row['image_gs']})"
        )

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save filtered samplesheet
    filtered_path = output_dir / f"gs_filtered_{samplesheet_path.name}"
    gs_df.to_csv(filtered_path, index=False, quoting=1)

    logging.info(f"Saved filtered samplesheet to {filtered_path}")
    return filtered_path


def extract_dataset_id(response: str) -> str:
    """Extract dataset ID from Seqera Platform response."""
    pattern = r"with id '(.+?)'"
    match = re.search(pattern, response)
    if not match:
        raise ValueError(f"Dataset ID not found in response: {response}")
    return match.group(1)


def extract_url(text: str) -> str:
    """Extract the first URL from a given string."""
    url_pattern = r"(https?://[^\s]+)"
    match = re.search(url_pattern, text)
    if not match:
        raise ValueError(f"No URL found in text: {text}")
    return match.group(0)


def upload_dataset(
    platform: seqeraplatform.SeqeraPlatform,
    samplesheet_path: Path,
    workspace: str,
    dataset_name: str,
) -> str:
    """Upload samplesheet as a dataset to Seqera Platform."""
    logging.info(f"Uploading dataset '{dataset_name}' from {samplesheet_path}")

    response = platform.datasets(
        "add",
        str(samplesheet_path),
        "--workspace",
        workspace,
        "--name",
        dataset_name,
        "--description",
        "Filtered GS samplesheet for synapse staging pipeline",
        "--overwrite",
        "--header",
        to_json=True,
    )

    dataset_id = extract_dataset_id(response)
    logging.info(f"Dataset uploaded with ID: {dataset_id}")
    return dataset_id


def get_dataset_url(
    platform: seqeraplatform.SeqeraPlatform, dataset_id: str, workspace: str
) -> str:
    """Get the URL for an uploaded dataset."""
    logging.info(f"Getting URL for dataset {dataset_id}")

    response = platform.datasets(
        "url",
        "--id",
        dataset_id,
        "--workspace",
        workspace,
        to_json=True,
    )

    url = extract_url(response)
    logging.info(f"Dataset URL: {url}")
    return url


def create_params_file(
    dataset_url: str,
    run_name: str,
    staging_bucket: str,
    output_bucket: str,
    params_dir: Path,
) -> Path:
    """Create a parameters file for the nf-synapse pipeline."""
    params_dir.mkdir(exist_ok=True)

    params_content = f"""input: {dataset_url}
entry: "synstage"
outdir: "{output_bucket}/synstage/{run_name}"
"""

    params_file = params_dir / f"params_synstage_{run_name.split('_')[1]}.yaml"
    params_file.write_text(params_content)

    logging.info(f"Created parameters file: {params_file}")
    return params_file


def launch_synapse_pipeline(
    platform: seqeraplatform.SeqeraPlatform,
    workspace: str,
    compute_env: str,
    run_name: str,
    params_file: Path,
    pipeline_url: str,
    revision: str = "main",
) -> str:
    """Launch the nf-synapse pipeline with synstage entry point."""
    logging.info(f"Launching synapse staging pipeline run '{run_name}'")

    response = platform.launch(
        "--workspace",
        workspace,
        "--compute-env",
        compute_env,
        "--name",
        run_name,
        "--revision",
        revision,
        "--wait",
        "SUBMITTED",
        pipeline_url,
        "--params-file",
        str(params_file),
        to_json=True,
    )

    logging.info("Synapse staging pipeline launched successfully")
    return response


def cleanup_params_file(params_file: Path) -> None:
    """Remove the parameters file if it exists."""
    try:
        if params_file.exists():
            params_file.unlink()
            logging.info(f"Removed parameters file: {params_file}")
        else:
            logging.warning(f"Parameters file not found for cleanup: {params_file}")
    except Exception as e:
        logging.warning(f"Failed to remove parameters file {params_file}: {e}")


def cleanup_filtered_samplesheet(filtered_path: Path) -> None:
    """Remove the filtered samplesheet file if it exists."""
    try:
        if filtered_path.exists():
            filtered_path.unlink()
            logging.info(f"Removed filtered samplesheet: {filtered_path}")
        else:
            logging.warning(
                f"Filtered samplesheet not found for cleanup: {filtered_path}"
            )
    except Exception as e:
        logging.warning(f"Failed to remove filtered samplesheet {filtered_path}: {e}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Stage Google Cloud Storage files via nf-synapse pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--samplesheet",
        type=Path,
        default=DEFAULT_SAMPLESHEET,
        help="Path to the samplesheet CSV file",
    )

    parser.add_argument(
        "--workspace", default=DEFAULT_WORKSPACE, help="Seqera Platform workspace ID"
    )

    parser.add_argument(
        "--compute-env", default=DEFAULT_COMPUTE_ENV, help="Compute environment ID"
    )

    parser.add_argument(
        "--staging-bucket",
        default=DEFAULT_STAGING_BUCKET,
        help="S3 bucket for staging files",
    )

    parser.add_argument(
        "--output-bucket",
        default=DEFAULT_OUTPUT_BUCKET,
        help="S3 bucket for output files",
    )

    parser.add_argument(
        "--pipeline-url",
        default=DEFAULT_PIPELINE_URL,
        help="GitHub URL of the nf-synapse pipeline",
    )

    parser.add_argument(
        "--revision", default="main", help="Pipeline revision/branch to use"
    )

    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    parser.add_argument(
        "--keep-files",
        action="store_true",
        help="Keep temporary files after pipeline launch (default: remove them)",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("staging"),
        help="Directory for temporary filtered samplesheet",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.debug)

    try:
        # Validate inputs
        validate_samplesheet(args.samplesheet)

        # Filter for Google Cloud Storage files
        filtered_samplesheet = filter_gs_files(args.samplesheet, args.output_dir)

        # Generate unique identifiers
        run_id = generate_run_id()
        dataset_name = f"gs_synstage_samplesheet_{run_id}"
        run_name = f"gs_synstage_{run_id}"

        logging.info(f"Starting synapse staging pipeline with run ID: {run_id}")

        # Initialize Seqera Platform
        platform = seqeraplatform.SeqeraPlatform()

        # Upload filtered dataset
        dataset_id = upload_dataset(
            platform, filtered_samplesheet, args.workspace, dataset_name
        )

        # Get dataset URL
        dataset_url = get_dataset_url(platform, dataset_id, args.workspace)

        # Create parameters file
        params_dir = Path("params")
        params_file = create_params_file(
            dataset_url,
            run_name,
            args.staging_bucket,
            args.output_bucket,
            params_dir,
        )

        # Launch synapse staging pipeline
        result = launch_synapse_pipeline(
            platform,
            args.workspace,
            args.compute_env,
            run_name,
            params_file,
            args.pipeline_url,
            args.revision,
        )

        print(f"✓ Synapse staging pipeline launched successfully!")
        print(f"  Run name: {run_name}")
        print(f"  Dataset ID: {dataset_id}")
        print(f"  Filtered samplesheet: {filtered_samplesheet}")
        print(f"  Parameters file: {params_file}")
        print(f"  Staging location: {args.staging_bucket}/{run_name}")
        print(f"  Output location: {args.output_bucket}/{run_name}")
        print(f"  Result: {result}")
        print(f"\n📝 Note: Staged files will be available in the synstage/ subfolder")
        print(f"     within the staging directory after pipeline completion.")

        # Clean up temporary files unless user wants to keep them
        if args.keep_files:
            logging.info(f"Keeping temporary files:")
            logging.info(f"  - Parameters file: {params_file}")
            logging.info(f"  - Filtered samplesheet: {filtered_samplesheet}")
        else:
            cleanup_params_file(params_file)
            cleanup_filtered_samplesheet(filtered_samplesheet)

        return 0

    except Exception as e:
        logging.error(f"Synapse staging pipeline launch failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
