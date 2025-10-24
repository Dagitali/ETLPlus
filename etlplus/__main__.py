"""Command-line interface for ETLPlus."""

import argparse
import sys
import json
from etlplus import __version__
from etlplus.extract import extract
from etlplus.validate import validate
from etlplus.transform import transform
from etlplus.load import load


def create_parser():
    """Create the argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        prog="etlplus",
        description="ETLPlus - A Swiss Army knife for enabling simple ETL operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Extract command
    extract_parser = subparsers.add_parser(
        "extract",
        help="Extract data from sources (files, databases, REST APIs)",
    )
    extract_parser.add_argument(
        "source_type",
        choices=["file", "database", "api"],
        help="Type of source to extract from",
    )
    extract_parser.add_argument(
        "source",
        help="Source location (file path, database connection string, or API URL)",
    )
    extract_parser.add_argument(
        "-o", "--output",
        help="Output file to save extracted data (JSON format)",
    )
    extract_parser.add_argument(
        "--format",
        choices=["json", "csv", "xml"],
        default="json",
        help="Format of the file to extract (default: json)",
    )
    
    # Validate command
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate data from sources",
    )
    validate_parser.add_argument(
        "source",
        help="Data source to validate (file path or JSON string)",
    )
    validate_parser.add_argument(
        "--rules",
        help="Validation rules as JSON string",
    )
    
    # Transform command
    transform_parser = subparsers.add_parser(
        "transform",
        help="Transform data",
    )
    transform_parser.add_argument(
        "source",
        help="Data source to transform (file path or JSON string)",
    )
    transform_parser.add_argument(
        "--operations",
        help="Transformation operations as JSON string",
    )
    transform_parser.add_argument(
        "-o", "--output",
        help="Output file to save transformed data",
    )
    
    # Load command
    load_parser = subparsers.add_parser(
        "load",
        help="Load data to targets (files, databases, REST APIs)",
    )
    load_parser.add_argument(
        "source",
        help="Data source to load (file path or JSON string)",
    )
    load_parser.add_argument(
        "target_type",
        choices=["file", "database", "api"],
        help="Type of target to load to",
    )
    load_parser.add_argument(
        "target",
        help="Target location (file path, database connection string, or API URL)",
    )
    load_parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Format for file output (default: json)",
    )
    
    return parser


def main():
    """Main entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 0
    
    try:
        if args.command == "extract":
            data = extract(args.source_type, args.source, format=args.format)
            if args.output:
                with open(args.output, "w") as f:
                    json.dump(data, f, indent=2)
                print(f"Data extracted and saved to {args.output}")
            else:
                print(json.dumps(data, indent=2))
                
        elif args.command == "validate":
            rules = json.loads(args.rules) if args.rules else {}
            result = validate(args.source, rules)
            print(json.dumps(result, indent=2))
            
        elif args.command == "transform":
            operations = json.loads(args.operations) if args.operations else {}
            data = transform(args.source, operations)
            if args.output:
                with open(args.output, "w") as f:
                    json.dump(data, f, indent=2)
                print(f"Data transformed and saved to {args.output}")
            else:
                print(json.dumps(data, indent=2))
                
        elif args.command == "load":
            result = load(args.source, args.target_type, args.target, format=args.format)
            print(json.dumps(result, indent=2))
            
        return 0
        
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
