# CSV Reconciliation API

A production-lean FastAPI application for reconciling two CSV files with exact, fuzzy, or composite joins. This API provides a complete workflow: upload → reconcile → download results.

## ✨ Features

- **Seven Join Types**:
  - **Exact Join**: Standard pandas merge on specified keys
  - **Fuzzy Join**: RapidFuzz-based matching with configurable similarity threshold
  - **Composite Join**: Join on concatenated key pairs for complex matching
  - **Case-Insensitive Exact**: Exact match ignoring case and whitespace
  - **Phonetic Join**: Match names that sound similar using Double Metaphone
  - **Date Tolerance**: Match dates within ± N days tolerance
  - **Numeric Tolerance**: Match numbers within ± X amount tolerance

- **Robust File Handling**:
  - Support for `.csv` and `.csv.gz` files
  - UTF-8 with latin-1 fallback encoding
  - File size limits (10MB) and row limits (100,000)
  - Comprehensive error handling and validation

- **Complete Output**:
  - `merged.csv` - Successfully matched records
  - `diffs.csv` - Unmatched records from both files
  - `qa_log.csv` - Detailed reconciliation summary with metrics

- **Session Management**:
  - UUID-based sessions with automatic cleanup
  - Secure file storage in isolated directories
  - Preview endpoint for column inspection

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start the Server

```bash
uvicorn main:app --reload
```

The API will be available at `http://127.0.0.1:8000`

### 3. Explore the API

Visit `http://127.0.0.1:8000/docs` for the interactive OpenAPI documentation.

## 📚 API Endpoints

### 🔄 Core Workflow

#### 1. Upload Files
```http
POST /upload
Content-Type: multipart/form-data

file_a: <CSV file>
file_b: <CSV file>
```

**Response:**
```json
{
  "session_id": "uuid-string",
  "file_a_columns": ["id", "name", "email"],
  "file_b_columns": ["id", "name", "email"],
  "rows_a": 1000,
  "rows_b": 950
}
```

#### 2. Reconcile Data
```http
POST /reconcile
Content-Type: application/json

{
  "session_id": "uuid-string",
  "join_mode": "exact|fuzzy|composite",
  "left_key": "column_name_in_file_a",
  "right_key": "column_name_in_file_b",
  "left_key2": "second_column_a",    // for composite only
  "right_key2": "second_column_b",   // for composite only  
  "threshold": 90                    // 0-100, for fuzzy only
}
```

**Response:**
```json
{
  "ready_for_download": true,
  "timestamp": "2024-01-01T12:00:00",
  "join_mode": "exact",
  "threshold": 90,
  "rows_a": 1000,
  "rows_b": 950,
  "matches_count": 800,
  "only_in_a_count": 200,
  "only_in_b_count": 150,
  "duration_ms": 1250
}
```

#### 3. Download Results
```http
GET /download/{file_type}?session_id=uuid-string
```

Where `file_type` is one of:
- `merged` - Successfully matched records
- `diffs` - Unmatched records with source column
- `qa_log` - Reconciliation summary and metrics

### 🔍 Additional Endpoints

#### Preview Files
```http
GET /preview?session_id=uuid-string&n=100
```

Returns first N rows and column information for both uploaded files.

#### Health Check
```http
GET /health
```

Returns `{"ok": true}` if the service is healthy.

## 💡 Usage Examples

### Example 1: Exact Join

```bash
# 1. Upload files
curl -X POST "http://localhost:8000/upload" \
  -F "file_a=@sample_data/customers_a.csv" \
  -F "file_b=@sample_data/customers_b.csv"

# Response: {"session_id": "abc-123", ...}

# 2. Perform exact join on ID
curl -X POST "http://localhost:8000/reconcile" \
  -H "Content-Type: application/json" \
  -d '{
    "session_id": "abc-123",
    "join_mode": "exact",
    "left_key": "id",
    "right_key": "id"
  }'

# 3. Download merged results
curl "http://localhost:8000/download/merged?session_id=abc-123" \
  --output merged_results.csv
```

### Example 2: Fuzzy Join

```bash
curl -X POST "http://localhost:8000/reconcile" \
  -H "Content-Type: application/json" \
  -d '{
    "session_id": "abc-123",
    "join_mode": "fuzzy",
    "left_key": "name",
    "right_key": "name", 
    "threshold": 85
  }'
```

### Example 3: Composite Join

```bash
curl -X POST "http://localhost:8000/reconcile" \
  -H "Content-Type: application/json" \
  -d '{
    "session_id": "abc-123",
    "join_mode": "composite",
    "left_key": "first_name",
    "left_key2": "last_name",
    "right_key": "first",
    "right_key2": "last"
  }'
```

### Example 4: Case-Insensitive Exact

```bash
curl -X POST "http://localhost:8000/reconcile" \
  -H "Content-Type: application/json" \
  -d '{
    "session_id": "abc-123",
    "join_mode": "exact_ci",
    "left_key": "customer_name",
    "right_key": "name"
  }'
```

### Example 5: Phonetic Matching

```bash
curl -X POST "http://localhost:8000/reconcile" \
  -H "Content-Type: application/json" \
  -d '{
    "session_id": "abc-123",
    "join_mode": "phonetic", 
    "left_key": "first_name",
    "right_key": "name"
  }'
```

### Example 6: Date Tolerance

```bash
curl -X POST "http://localhost:8000/reconcile" \
  -H "Content-Type: application/json" \
  -d '{
    "session_id": "abc-123",
    "join_mode": "date_tolerance",
    "left_date_key": "order_date",
    "right_date_key": "shipment_date", 
    "tolerance_days": 7,
    "left_secondary_key": "customer_id",
    "right_secondary_key": "cust_id"
  }'
```

### Example 7: Numeric Tolerance

```bash
curl -X POST "http://localhost:8000/reconcile" \
  -H "Content-Type: application/json" \
  -d '{
    "session_id": "abc-123",
    "join_mode": "numeric_tolerance",
    "left_num_key": "amount",
    "right_num_key": "total",
    "tolerance_amount": 5.0
  }'
```

## 📁 Output Files

### merged.csv
Contains successfully matched records from both files. Column naming:
- Unique columns keep original names
- Conflicting columns get `_a` and `_b` suffixes
- **Fuzzy joins**: include a `match_score` column (0-100)
- **Phonetic joins**: include a `match_key` column set to "phonetic"
- **Date tolerance**: include a `date_diff_days` column (signed difference in days)
- **Numeric tolerance**: include a `num_diff` column (signed numeric difference)

### diffs.csv  
Contains unmatched records with a `source` column indicating:
- `only_in_a` - Records only found in file A
- `only_in_b` - Records only found in file B

### qa_log.csv
Single-row summary with columns:
- `timestamp` - When reconciliation was performed
- `join_mode` - Type of join performed
- `threshold` - Fuzzy matching threshold (if applicable)
- `tolerance_days` - Date tolerance in days (if applicable)
- `tolerance_amount` - Numeric tolerance amount (if applicable)
- `rows_a`, `rows_b` - Input file row counts
- `matches_count` - Number of successful matches
- `only_in_a_count`, `only_in_b_count` - Unmatched record counts
- `duration_ms` - Processing time in milliseconds
- `date_diff_min/max/mean` - Date difference statistics (for date_tolerance)
- `num_diff_min/max/mean` - Numeric difference statistics (for numeric_tolerance)

## ⚙️ Configuration

### File Limits
- **Max file size**: 10MB per file
- **Max rows**: 100,000 rows per file
- **Supported formats**: `.csv`, `.csv.gz`
- **Encoding**: UTF-8 (fallback to latin-1)

### Session Management
- Sessions auto-expire after 6 hours
- Session cleanup runs on server startup
- Each session gets isolated storage in `./sessions/<uuid>/`

### Fuzzy Matching
- Uses RapidFuzz `partial_ratio` algorithm
- Configurable threshold (0-100)
- Default threshold: 90
- One-to-one matching (best match per record)

### Phonetic Matching
- Uses Jellyfish Double Metaphone algorithm
- Matches names that sound similar (Steven ↔ Stephen)
- Case-insensitive and handles common name variations

### Date/Numeric Tolerance
- **Date tolerance**: Uses pandas `merge_asof` with configurable day tolerance
- **Numeric tolerance**: Nearest-neighbor matching within specified range
- Optional secondary keys for grouped date matching
- Difference statistics included in results

## 🧪 Testing

Run the comprehensive test suite:

```bash
pytest tests/test_reconcile.py -v
```

Test coverage includes:
- Upload validation and error handling
- All three join modes with various scenarios
- File download functionality
- Preview endpoint
- Error cases and edge conditions

### Sample Data

The project includes sample CSV files in `sample_data/`:
- `customers_a.csv` - 3 records with id, name, email
- `customers_b.csv` - 3 records with slight differences

Perfect for testing the API functionality.

## 🛠️ Development

### Project Structure
```
├── main.py              # FastAPI application
├── requirements.txt     # Python dependencies  
├── tests/
│   └── test_reconcile.py # Comprehensive test suite
├── sample_data/
│   ├── customers_a.csv  # Sample input file A
│   └── customers_b.csv  # Sample input file B
├── sessions/            # Session storage (auto-created)
├── .gitignore
└── README.md
```

### Key Functions

- `load_csv_safely()` - CSV loading with encoding handling
- `exact_join()` - Standard pandas merge
- `fuzzy_join()` - RapidFuzz-based matching with scoring
- `composite_join()` - Multi-column key concatenation
- `build_diffs()` - Anti-join for unmatched records
- `write_outputs()` - Generate all output files

## 🔒 Security & Limits

- **File validation**: Extension and content checks
- **Size limits**: Prevents memory exhaustion
- **Path sanitization**: UUID-based session directories
- **Automatic cleanup**: Old sessions removed on startup
- **Input validation**: Pydantic models for request validation

## 🚨 Error Handling

The API provides helpful error messages for common issues:

- **400 Bad Request**: Invalid file format, size exceeded, missing columns
- **404 Not Found**: Session expired or files not found
- **422 Validation Error**: Invalid join mode, threshold, or missing required fields
- **500 Server Error**: Unexpected processing errors

## 📖 API Documentation

Once the server is running, visit:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

Both provide interactive documentation where you can test all endpoints directly in your browser.

---

## 📄 License

This project is open source. Feel free to use and modify as needed.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

---

**Need help?** Check the interactive API documentation at `/docs` or examine the test cases for usage examples. 