# Data Sharing Project

This project is designed to facilitate secure and efficient data sharing between different systems and services.

## Features

- **Data Pipeline**: Processes and loads transaction data from various sources.
- **Dashboard**: Provides visualization of transaction data.
- **Configuration**: Supports multiple cloud providers (AWS, Snowflake).

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/kaustubhuk8/DataShare.git
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure your environment:
   - Copy `configs/aws_config.json.example` to `configs/aws_config.json`
   - Copy `configs/snowflake_config.json.example` to `configs/snowflake_config.json`
   - Fill in your credentials

4. Run the pipeline:
   ```bash
   python pipeline_script.sh
   ```

## Project Structure

```
.
├── configs/            # Configuration files
│   ├── aws_config.json
│   └── snowflake_config.json
├── data/               # Data files
├── logs/               # Log files
├── scripts/            # Pipeline scripts
├── dashboard.py        # Dashboard application
├── requirements.txt    # Python dependencies
└── README.md           # This file
```

## Contributing

1. Fork the repository
2. Create a new branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT
