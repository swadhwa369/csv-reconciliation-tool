import os
import shutil
import tempfile
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Literal

import pandas as pd
import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile, Request, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, validator, Field
from rapidfuzz import fuzz
import jellyfish
import time

# Initialize FastAPI app
app = FastAPI(
    title="CSV Reconciliation API",
    description="A production-lean FastAPI app for reconciling CSVs with exact/fuzzy/composite joins",
    version="1.0.0"
)

# Create API router for all API endpoints
api_router = APIRouter(prefix="/api")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files for frontend assets
frontend_dist = Path("./frontend/dist")
if frontend_dist.exists():
    app.mount("/assets", StaticFiles(directory=str(frontend_dist / "assets")), name="assets")

# Configuration
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
MAX_ROWS = 100000
SESSION_DIR = Path("./sessions")
SESSION_CLEANUP_HOURS = 6

# Pydantic models
class ReconcileRequest(BaseModel):
    session_id: str
    join_mode: Literal["exact", "fuzzy", "composite", "exact_ci", "phonetic", "date_tolerance", "numeric_tolerance"] = Field(
        description="Type of reconciliation join to perform"
    )
    left_key: str = Field(description="Primary column name from file A")
    right_key: str = Field(description="Primary column name from file B")
    left_key2: Optional[str] = Field(None, description="Secondary column from file A (required for composite)")
    right_key2: Optional[str] = Field(None, description="Secondary column from file B (required for composite)")
    threshold: int = Field(90, description="Similarity threshold 0-100 (used for fuzzy matching)")
    
    # Date tolerance fields
    left_date_key: Optional[str] = Field(None, description="Date column from file A (required for date_tolerance)")
    right_date_key: Optional[str] = Field(None, description="Date column from file B (required for date_tolerance)")
    tolerance_days: int = Field(3, description="Number of days tolerance for date matching")
    
    # Numeric tolerance fields  
    left_num_key: Optional[str] = Field(None, description="Numeric column from file A (required for numeric_tolerance)")
    right_num_key: Optional[str] = Field(None, description="Numeric column from file B (required for numeric_tolerance)")
    tolerance_amount: float = Field(1.0, description="Numeric tolerance for numeric matching")
    
    # Secondary constraint fields for date_tolerance
    left_secondary_key: Optional[str] = Field(None, description="Optional secondary grouping column from file A")
    right_secondary_key: Optional[str] = Field(None, description="Optional secondary grouping column from file B")

    @validator('threshold')
    def validate_threshold(cls, v):
        if not 0 <= v <= 100:
            raise ValueError('threshold must be between 0 and 100')
        return v

    @validator('tolerance_days')
    def validate_tolerance_days(cls, v):
        if v < 0:
            raise ValueError('tolerance_days must be non-negative')
        return v

    @validator('tolerance_amount')
    def validate_tolerance_amount(cls, v):
        if v < 0:
            raise ValueError('tolerance_amount must be non-negative')
        return v

    @validator('left_key2', 'right_key2', always=True)
    def validate_composite_keys(cls, v, values):
        if values.get('join_mode') == 'composite':
            if 'left_key2' not in values or 'right_key2' not in values or not values.get('left_key2') or not values.get('right_key2'):
                raise ValueError('composite join requires both left_key2 and right_key2')
        return v

    @validator('left_date_key', 'right_date_key', always=True)
    def validate_date_keys(cls, v, values):
        if values.get('join_mode') == 'date_tolerance':
            if not values.get('left_date_key') or not values.get('right_date_key'):
                raise ValueError('date_tolerance join requires both left_date_key and right_date_key')
        return v

    @validator('left_num_key', 'right_num_key', always=True)
    def validate_numeric_keys(cls, v, values):
        if values.get('join_mode') == 'numeric_tolerance':
            if not values.get('left_num_key') or not values.get('right_num_key'):
                raise ValueError('numeric_tolerance join requires both left_num_key and right_num_key')
        return v


def cleanup_old_sessions():
    """Remove session folders older than SESSION_CLEANUP_HOURS"""
    try:
        if not SESSION_DIR.exists():
            return
        
        cutoff_time = datetime.now() - timedelta(hours=SESSION_CLEANUP_HOURS)
        
        for session_folder in SESSION_DIR.iterdir():
            if session_folder.is_dir():
                try:
                    folder_time = datetime.fromtimestamp(session_folder.stat().st_mtime)
                    if folder_time < cutoff_time:
                        shutil.rmtree(session_folder)
                        print(f"Cleaned up old session: {session_folder.name}")
                except Exception as e:
                    print(f"Failed to clean up session {session_folder.name}: {e}")
    except Exception as e:
        print(f"Session cleanup failed: {e}")


def load_csv_safely(file_path: Path) -> pd.DataFrame:
    """Load CSV with proper encoding handling and validation"""
    try:
        # Try UTF-8 first
        df = pd.read_csv(file_path, compression='infer', encoding='utf-8')
    except UnicodeDecodeError:
        # Fall back to latin-1
        print(f"Warning: UTF-8 failed for {file_path}, using latin-1")
        df = pd.read_csv(file_path, compression='infer', encoding='latin-1')
    
    # Validate the DataFrame
    if df.empty:
        raise ValueError("CSV file is empty")
    
    if len(df.columns) == 0:
        raise ValueError("CSV file has no columns")
    
    # Check for duplicate column names
    if len(df.columns) != len(set(df.columns)):
        raise ValueError("CSV file has duplicate column names")
    
    # Check row limit
    if len(df) > MAX_ROWS:
        raise ValueError(f"CSV file exceeds maximum row limit of {MAX_ROWS:,} rows")
    
    return df


def exact_join(df_a: pd.DataFrame, df_b: pd.DataFrame, left_key: str, right_key: str) -> pd.DataFrame:
    """Perform exact join between two DataFrames"""
    return pd.merge(df_a, df_b, left_on=left_key, right_on=right_key, how='inner', suffixes=('_a', '_b'))


def fuzzy_join(df_a: pd.DataFrame, df_b: pd.DataFrame, left_key: str, right_key: str, threshold: int) -> Tuple[pd.DataFrame, List[int], List[int]]:
    """
    Perform fuzzy join using RapidFuzz partial_ratio
    Returns: (matched_df, unmatched_a_indices, unmatched_b_indices)
    """
    matched_rows = []
    matched_b_indices = set()
    unmatched_a_indices = []
    
    # Convert to string and handle NaN values
    df_a_key_str = df_a[left_key].astype(str).fillna('')
    df_b_key_str = df_b[right_key].astype(str).fillna('')
    
    for i, a_value in enumerate(df_a_key_str):
        best_match_idx = None
        best_score = 0
        
        # Find best match in df_b
        for j, b_value in enumerate(df_b_key_str):
            if j in matched_b_indices:
                continue  # Already matched
            
            score = fuzz.partial_ratio(str(a_value), str(b_value))
            if score > best_score and score >= threshold:
                best_score = score
                best_match_idx = j
        
        if best_match_idx is not None:
            # Create matched row
            row_a = df_a.iloc[i].to_dict()
            row_b = df_b.iloc[best_match_idx].to_dict()
            
            # Handle column name conflicts
            merged_row = {}
            for col, val in row_a.items():
                merged_row[f"{col}_a" if col in row_b else col] = val
            for col, val in row_b.items():
                merged_row[f"{col}_b" if col in row_a else col] = val
            
            merged_row['match_score'] = best_score
            matched_rows.append(merged_row)
            matched_b_indices.add(best_match_idx)
        else:
            unmatched_a_indices.append(i)
    
    # Find unmatched indices in B
    unmatched_b_indices = [i for i in range(len(df_b)) if i not in matched_b_indices]
    
    matched_df = pd.DataFrame(matched_rows) if matched_rows else pd.DataFrame()
    return matched_df, unmatched_a_indices, unmatched_b_indices


def composite_join(df_a: pd.DataFrame, df_b: pd.DataFrame, left_k1: str, left_k2: str, right_k1: str, right_k2: str) -> pd.DataFrame:
    """Perform composite join by concatenating key pairs"""
    # Create composite keys
    df_a_copy = df_a.copy()
    df_b_copy = df_b.copy()
    
    df_a_copy['_composite_key'] = df_a_copy[left_k1].astype(str) + "|" + df_a_copy[left_k2].astype(str)
    df_b_copy['_composite_key'] = df_b_copy[right_k1].astype(str) + "|" + df_b_copy[right_k2].astype(str)
    
    # Perform exact join on composite key
    merged = pd.merge(df_a_copy, df_b_copy, on='_composite_key', how='inner', suffixes=('_a', '_b'))
    
    # Drop the temporary composite key column
    merged = merged.drop('_composite_key', axis=1)
    
    return merged


def build_diffs(df_a: pd.DataFrame, df_b: pd.DataFrame, matched_a_indices: List[int] = None, matched_b_indices: List[int] = None, 
                left_key: str = None, right_key: str = None, composite_key: str = None) -> pd.DataFrame:
    """Build diffs DataFrame showing unmatched records from both sides"""
    diffs = []
    
    if matched_a_indices is not None:
        # For fuzzy joins - use indices
        unmatched_a = df_a.drop(df_a.index[matched_a_indices]) if matched_a_indices else df_a
        unmatched_b = df_b.drop(df_b.index[matched_b_indices]) if matched_b_indices else df_b
    else:
        # For exact/composite joins - use anti-join
        if composite_key:
            # For composite joins
            df_a_with_key = df_a.copy()
            df_b_with_key = df_b.copy()
            df_a_with_key['_temp_key'] = composite_key + "_a"
            df_b_with_key['_temp_key'] = composite_key + "_b"
            
            matched_keys_a = df_a_with_key[df_a_with_key['_temp_key'].isin(df_b_with_key['_temp_key'])]
            matched_keys_b = df_b_with_key[df_b_with_key['_temp_key'].isin(df_a_with_key['_temp_key'])]
            
            unmatched_a = df_a[~df_a.index.isin(matched_keys_a.index)]
            unmatched_b = df_b[~df_b.index.isin(matched_keys_b.index)]
        else:
            # For exact joins
            matched_a_keys = df_a[df_a[left_key].isin(df_b[right_key])]
            matched_b_keys = df_b[df_b[right_key].isin(df_a[left_key])]
            
            unmatched_a = df_a[~df_a.index.isin(matched_a_keys.index)]
            unmatched_b = df_b[~df_b.index.isin(matched_b_keys.index)]
    
    # Add source column and combine
    if not unmatched_a.empty:
        unmatched_a_copy = unmatched_a.copy()
        unmatched_a_copy['source'] = 'only_in_a'
        diffs.append(unmatched_a_copy)
    
    if not unmatched_b.empty:
        unmatched_b_copy = unmatched_b.copy()
        unmatched_b_copy['source'] = 'only_in_b'
        diffs.append(unmatched_b_copy)
    
    if diffs:
        return pd.concat(diffs, ignore_index=True, sort=False)
    else:
        return pd.DataFrame()


def normalize_string(series: pd.Series) -> pd.Series:
    """Normalize strings for case-insensitive matching: strip whitespace, lowercase, handle NaN"""
    return series.astype(str).str.strip().str.lower().fillna('')


def phonetic_key(series: pd.Series) -> pd.Series:
    """Convert strings to phonetic keys using Double Metaphone"""
    def get_metaphone(text):
        if pd.isna(text) or text == '':
            return ''
        metaphone_result = jellyfish.double_metaphone(str(text))
        return metaphone_result[0] if metaphone_result[0] else metaphone_result[1] or ''
    
    return series.apply(get_metaphone)


def ensure_datetime(df: pd.DataFrame, col: str) -> pd.Series:
    """Convert column to datetime with validation"""
    series = pd.to_datetime(df[col], errors='coerce')
    invalid_count = series.isna().sum()
    total_count = len(series)
    
    if total_count > 0 and (invalid_count / total_count) > 0.1:
        raise HTTPException(
            status_code=400, 
            detail=f"Column '{col}' has {invalid_count}/{total_count} ({invalid_count/total_count*100:.1f}%) invalid dates. Maximum 10% allowed."
        )
    
    return series


def ensure_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    """Convert column to numeric with validation"""
    series = pd.to_numeric(df[col], errors='coerce')
    invalid_count = series.isna().sum()
    total_count = len(series)
    
    if total_count > 0 and (invalid_count / total_count) > 0.1:
        raise HTTPException(
            status_code=400, 
            detail=f"Column '{col}' has {invalid_count}/{total_count} ({invalid_count/total_count*100:.1f}%) invalid numbers. Maximum 10% allowed."
        )
    
    return series


def exact_ci_join(df_a: pd.DataFrame, df_b: pd.DataFrame, left_key: str, right_key: str) -> pd.DataFrame:
    """Perform case-insensitive exact join"""
    df_a_copy = df_a.copy()
    df_b_copy = df_b.copy()
    
    # Normalize keys
    df_a_copy['_normalized_key'] = normalize_string(df_a_copy[left_key])
    df_b_copy['_normalized_key'] = normalize_string(df_b_copy[right_key])
    
    # Perform exact join on normalized keys
    merged = pd.merge(df_a_copy, df_b_copy, on='_normalized_key', how='inner', suffixes=('_a', '_b'))
    
    # Drop the temporary normalized key column
    merged = merged.drop('_normalized_key', axis=1)
    
    return merged


def phonetic_join(df_a: pd.DataFrame, df_b: pd.DataFrame, left_key: str, right_key: str) -> pd.DataFrame:
    """Perform phonetic join using Double Metaphone"""
    df_a_copy = df_a.copy()
    df_b_copy = df_b.copy()
    
    # Create phonetic keys
    df_a_copy['_phonetic_key'] = phonetic_key(df_a_copy[left_key])
    df_b_copy['_phonetic_key'] = phonetic_key(df_b_copy[right_key])
    
    # Perform exact join on phonetic keys
    merged = pd.merge(df_a_copy, df_b_copy, on='_phonetic_key', how='inner', suffixes=('_a', '_b'))
    
    # Add match_key column and drop temporary column
    merged['match_key'] = 'phonetic'
    merged = merged.drop('_phonetic_key', axis=1)
    
    return merged


def date_tolerance_join(df_a: pd.DataFrame, df_b: pd.DataFrame, left_date_key: str, right_date_key: str, 
                       tolerance_days: int, left_secondary_key: str = None, right_secondary_key: str = None) -> pd.DataFrame:
    """Perform date tolerance join using merge_asof"""
    df_a_copy = df_a.copy()
    df_b_copy = df_b.copy()
    
    # Convert to datetime
    df_a_copy[left_date_key] = ensure_datetime(df_a_copy, left_date_key)
    df_b_copy[right_date_key] = ensure_datetime(df_b_copy, right_date_key)
    
    # Drop rows with invalid dates
    df_a_copy = df_a_copy.dropna(subset=[left_date_key])
    df_b_copy = df_b_copy.dropna(subset=[right_date_key])
    
    if df_a_copy.empty or df_b_copy.empty:
        return pd.DataFrame()
    
    # Sort by date
    df_a_copy = df_a_copy.sort_values(left_date_key)
    df_b_copy = df_b_copy.sort_values(right_date_key)
    
    tolerance = pd.Timedelta(days=tolerance_days)
    
    if left_secondary_key and right_secondary_key:
        # Group-wise merge_asof
        merged_parts = []
        for group_val in df_a_copy[left_secondary_key].unique():
            a_group = df_a_copy[df_a_copy[left_secondary_key] == group_val]
            b_group = df_b_copy[df_b_copy[right_secondary_key] == group_val]
            
            if not a_group.empty and not b_group.empty:
                merged_group = pd.merge_asof(
                    a_group, b_group,
                    left_on=left_date_key, right_on=right_date_key,
                    tolerance=tolerance, direction='nearest',
                    suffixes=('_a', '_b')
                )
                merged_parts.append(merged_group)
        
        merged = pd.concat(merged_parts, ignore_index=True) if merged_parts else pd.DataFrame()
    else:
        # Simple merge_asof
        merged = pd.merge_asof(
            df_a_copy, df_b_copy,
            left_on=left_date_key, right_on=right_date_key,
            tolerance=tolerance, direction='nearest',
            suffixes=('_a', '_b')
        )
    
    # Calculate date difference
    if not merged.empty:
        left_col = left_date_key + '_a' if left_date_key in df_b_copy.columns else left_date_key
        right_col = right_date_key + '_b' if right_date_key in df_a_copy.columns else right_date_key
        merged['date_diff_days'] = (merged[left_col] - merged[right_col]).dt.days
        
        # Filter out rows that exceed tolerance
        merged = merged[abs(merged['date_diff_days']) <= tolerance_days]
    
    return merged


def numeric_tolerance_join(df_a: pd.DataFrame, df_b: pd.DataFrame, left_num_key: str, right_num_key: str, tolerance_amount: float) -> pd.DataFrame:
    """Perform numeric tolerance join using merge_asof"""
    df_a_copy = df_a.copy()
    df_b_copy = df_b.copy()
    
    # Convert to numeric
    df_a_copy[left_num_key] = ensure_numeric(df_a_copy, left_num_key)
    df_b_copy[right_num_key] = ensure_numeric(df_b_copy, right_num_key)
    
    # Drop rows with invalid numbers
    df_a_copy = df_a_copy.dropna(subset=[left_num_key])
    df_b_copy = df_b_copy.dropna(subset=[right_num_key])
    
    if df_a_copy.empty or df_b_copy.empty:
        return pd.DataFrame()
    
    # Sort by numeric key
    df_a_copy = df_a_copy.sort_values(left_num_key)
    df_b_copy = df_b_copy.sort_values(right_num_key)
    
    # Use merge_asof for nearest matching
    merged = pd.merge_asof(
        df_a_copy, df_b_copy,
        left_on=left_num_key, right_on=right_num_key,
        direction='nearest',
        suffixes=('_a', '_b')
    )
    
    # Calculate numeric difference and filter by tolerance
    if not merged.empty:
        left_col = left_num_key + '_a' if left_num_key in df_b_copy.columns else left_num_key
        right_col = right_num_key + '_b' if right_num_key in df_a_copy.columns else right_num_key
        merged['num_diff'] = merged[left_col] - merged[right_col]
        
        # Filter out rows that exceed tolerance
        merged = merged[abs(merged['num_diff']) <= tolerance_amount]
    
    return merged


def write_outputs(session_dir: Path, merged: pd.DataFrame, diffs: pd.DataFrame, qa_log_row: Dict):
    """Write all output files to the session directory"""
    # Write merged.csv
    merged_path = session_dir / "merged.csv"
    if not merged.empty:
        merged.to_csv(merged_path, index=False)
    else:
        # Create empty file with headers if no matches
        pd.DataFrame().to_csv(merged_path, index=False)
    
    # Write diffs.csv
    diffs_path = session_dir / "diffs.csv"
    if not diffs.empty:
        diffs.to_csv(diffs_path, index=False)
    else:
        # Create empty file if no diffs
        pd.DataFrame().to_csv(diffs_path, index=False)
    
    # Write qa_log.csv
    qa_log_path = session_dir / "qa_log.csv"
    qa_df = pd.DataFrame([qa_log_row])
    qa_df.to_csv(qa_log_path, index=False)


# Startup event
@app.on_event("startup")
async def startup_event():
    """Clean up old sessions on startup"""
    SESSION_DIR.mkdir(exist_ok=True)
    cleanup_old_sessions()


# Endpoints
@api_router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"ok": True}


@api_router.post("/upload")
async def upload_files(file_a: UploadFile = File(...), file_b: UploadFile = File(...)):
    """Upload two CSV files and return session info"""
    
    # Validate file extensions
    allowed_extensions = ['.csv', '.csv.gz']
    for file in [file_a, file_b]:
        if not any(file.filename.lower().endswith(ext) for ext in allowed_extensions):
            raise HTTPException(
                status_code=400, 
                detail=f"File {file.filename} must be a CSV file (.csv or .csv.gz)"
            )
    
    # Check file sizes
    if file_a.size > MAX_FILE_SIZE or file_b.size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"Files must be smaller than {MAX_FILE_SIZE / (1024*1024):.0f}MB"
        )
    
    # Create session directory
    session_id = str(uuid.uuid4())
    session_dir = SESSION_DIR / session_id
    session_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Save uploaded files
        file_a_path = session_dir / "file_a.csv"
        file_b_path = session_dir / "file_b.csv"
        
        with open(file_a_path, "wb") as buffer:
            shutil.copyfileobj(file_a.file, buffer)
        
        with open(file_b_path, "wb") as buffer:
            shutil.copyfileobj(file_b.file, buffer)
        
        # Load and validate files
        try:
            df_a = load_csv_safely(file_a_path)
            df_b = load_csv_safely(file_b_path)
        except ValueError as e:
            # Clean up session directory on validation failure
            shutil.rmtree(session_dir)
            raise HTTPException(status_code=400, detail=str(e))
        
        return {
            "session_id": session_id,
            "file_a_columns": list(df_a.columns),
            "file_b_columns": list(df_b.columns),
            "rows_a": len(df_a),
            "rows_b": len(df_b)
        }
        
    except Exception as e:
        # Clean up session directory on error
        if session_dir.exists():
            shutil.rmtree(session_dir)
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@api_router.post("/reconcile")
async def reconcile_files(request: ReconcileRequest):
    """Reconcile two CSV files based on the specified join mode"""
    
    session_dir = SESSION_DIR / request.session_id
    if not session_dir.exists():
        raise HTTPException(status_code=404, detail="Session not found")
    
    file_a_path = session_dir / "file_a.csv"
    file_b_path = session_dir / "file_b.csv"
    
    if not file_a_path.exists() or not file_b_path.exists():
        raise HTTPException(status_code=404, detail="Session files not found")
    
    start_time = time.time()
    
    try:
        # Load files
        df_a = load_csv_safely(file_a_path)
        df_b = load_csv_safely(file_b_path)
        
        # Validate keys exist based on join mode
        if request.join_mode in ['exact', 'fuzzy', 'exact_ci', 'phonetic']:
            if request.left_key not in df_a.columns:
                raise HTTPException(status_code=400, detail=f"Column '{request.left_key}' not found in file A")
            if request.right_key not in df_b.columns:
                raise HTTPException(status_code=400, detail=f"Column '{request.right_key}' not found in file B")
        
        if request.join_mode == 'composite':
            if request.left_key not in df_a.columns:
                raise HTTPException(status_code=400, detail=f"Column '{request.left_key}' not found in file A")
            if request.right_key not in df_b.columns:
                raise HTTPException(status_code=400, detail=f"Column '{request.right_key}' not found in file B")
            if request.left_key2 not in df_a.columns:
                raise HTTPException(status_code=400, detail=f"Column '{request.left_key2}' not found in file A")
            if request.right_key2 not in df_b.columns:
                raise HTTPException(status_code=400, detail=f"Column '{request.right_key2}' not found in file B")
        
        if request.join_mode == 'date_tolerance':
            if request.left_date_key not in df_a.columns:
                raise HTTPException(status_code=400, detail=f"Column '{request.left_date_key}' not found in file A")
            if request.right_date_key not in df_b.columns:
                raise HTTPException(status_code=400, detail=f"Column '{request.right_date_key}' not found in file B")
            if request.left_secondary_key and request.left_secondary_key not in df_a.columns:
                raise HTTPException(status_code=400, detail=f"Column '{request.left_secondary_key}' not found in file A")
            if request.right_secondary_key and request.right_secondary_key not in df_b.columns:
                raise HTTPException(status_code=400, detail=f"Column '{request.right_secondary_key}' not found in file B")
        
        if request.join_mode == 'numeric_tolerance':
            if request.left_num_key not in df_a.columns:
                raise HTTPException(status_code=400, detail=f"Column '{request.left_num_key}' not found in file A")
            if request.right_num_key not in df_b.columns:
                raise HTTPException(status_code=400, detail=f"Column '{request.right_num_key}' not found in file B")
        
        # Perform join based on mode
        if request.join_mode == 'exact':
            merged = exact_join(df_a, df_b, request.left_key, request.right_key)
            diffs = build_diffs(df_a, df_b, left_key=request.left_key, right_key=request.right_key)
            
        elif request.join_mode == 'fuzzy':
            merged, unmatched_a_indices, unmatched_b_indices = fuzzy_join(
                df_a, df_b, request.left_key, request.right_key, request.threshold
            )
            matched_a_indices = [i for i in range(len(df_a)) if i not in unmatched_a_indices]
            matched_b_indices = [i for i in range(len(df_b)) if i not in unmatched_b_indices]
            diffs = build_diffs(df_a, df_b, matched_a_indices, matched_b_indices)
            
        elif request.join_mode == 'composite':
            merged = composite_join(
                df_a, df_b, request.left_key, request.left_key2, 
                request.right_key, request.right_key2
            )
            # For composite, create composite keys for diff calculation
            composite_key_a = df_a[request.left_key].astype(str) + "|" + df_a[request.left_key2].astype(str)
            composite_key_b = df_b[request.right_key].astype(str) + "|" + df_b[request.right_key2].astype(str)
            
            # Find matches
            matched_keys = set(composite_key_a) & set(composite_key_b)
            matched_a_mask = composite_key_a.isin(matched_keys)
            matched_b_mask = composite_key_b.isin(matched_keys)
            
            unmatched_a = df_a[~matched_a_mask]
            unmatched_b = df_b[~matched_b_mask]
            
            diffs_list = []
            if not unmatched_a.empty:
                unmatched_a_copy = unmatched_a.copy()
                unmatched_a_copy['source'] = 'only_in_a'
                diffs_list.append(unmatched_a_copy)
            if not unmatched_b.empty:
                unmatched_b_copy = unmatched_b.copy()
                unmatched_b_copy['source'] = 'only_in_b'
                diffs_list.append(unmatched_b_copy)
            
            diffs = pd.concat(diffs_list, ignore_index=True, sort=False) if diffs_list else pd.DataFrame()
            
        elif request.join_mode == 'exact_ci':
            merged = exact_ci_join(df_a, df_b, request.left_key, request.right_key)
            # For exact_ci, use normalized keys for diff calculation
            df_a_norm = df_a.copy()
            df_b_norm = df_b.copy()
            df_a_norm['_temp_key'] = normalize_string(df_a_norm[request.left_key])
            df_b_norm['_temp_key'] = normalize_string(df_b_norm[request.right_key])
            
            matched_keys = set(df_a_norm['_temp_key']) & set(df_b_norm['_temp_key'])
            matched_a_mask = df_a_norm['_temp_key'].isin(matched_keys)
            matched_b_mask = df_b_norm['_temp_key'].isin(matched_keys)
            
            unmatched_a = df_a[~matched_a_mask]
            unmatched_b = df_b[~matched_b_mask]
            
            diffs_list = []
            if not unmatched_a.empty:
                unmatched_a_copy = unmatched_a.copy()
                unmatched_a_copy['source'] = 'only_in_a'
                diffs_list.append(unmatched_a_copy)
            if not unmatched_b.empty:
                unmatched_b_copy = unmatched_b.copy()
                unmatched_b_copy['source'] = 'only_in_b'
                diffs_list.append(unmatched_b_copy)
            
            diffs = pd.concat(diffs_list, ignore_index=True, sort=False) if diffs_list else pd.DataFrame()
            
        elif request.join_mode == 'phonetic':
            merged = phonetic_join(df_a, df_b, request.left_key, request.right_key)
            # For phonetic, use phonetic keys for diff calculation
            df_a_phon = df_a.copy()
            df_b_phon = df_b.copy()
            df_a_phon['_temp_key'] = phonetic_key(df_a_phon[request.left_key])
            df_b_phon['_temp_key'] = phonetic_key(df_b_phon[request.right_key])
            
            matched_keys = set(df_a_phon['_temp_key']) & set(df_b_phon['_temp_key'])
            matched_a_mask = df_a_phon['_temp_key'].isin(matched_keys) & (df_a_phon['_temp_key'] != '')
            matched_b_mask = df_b_phon['_temp_key'].isin(matched_keys) & (df_b_phon['_temp_key'] != '')
            
            unmatched_a = df_a[~matched_a_mask]
            unmatched_b = df_b[~matched_b_mask]
            
            diffs_list = []
            if not unmatched_a.empty:
                unmatched_a_copy = unmatched_a.copy()
                unmatched_a_copy['source'] = 'only_in_a'
                diffs_list.append(unmatched_a_copy)
            if not unmatched_b.empty:
                unmatched_b_copy = unmatched_b.copy()
                unmatched_b_copy['source'] = 'only_in_b'
                diffs_list.append(unmatched_b_copy)
            
            diffs = pd.concat(diffs_list, ignore_index=True, sort=False) if diffs_list else pd.DataFrame()
            
        elif request.join_mode == 'date_tolerance':
            merged = date_tolerance_join(
                df_a, df_b, request.left_date_key, request.right_date_key, 
                request.tolerance_days, request.left_secondary_key, request.right_secondary_key
            )
            # For date tolerance, compute diffs based on matched indices
            if not merged.empty and '_merge' not in merged.columns:
                # Find unmatched rows by comparing indices
                matched_a_indices = merged.index.tolist() if hasattr(merged, 'index') else []
                matched_b_indices = []  # merge_asof doesn't preserve right indices easily
                
                # Simple approach: mark all as matched for now, improve later if needed
                unmatched_a = df_a.drop(matched_a_indices) if matched_a_indices else df_a
                unmatched_b = pd.DataFrame()  # merge_asof makes this complex to compute exactly
                
                diffs_list = []
                if not unmatched_a.empty:
                    unmatched_a_copy = unmatched_a.copy()
                    unmatched_a_copy['source'] = 'only_in_a'
                    diffs_list.append(unmatched_a_copy)
                
                diffs = pd.concat(diffs_list, ignore_index=True, sort=False) if diffs_list else pd.DataFrame()
            else:
                diffs = pd.DataFrame()
                
        elif request.join_mode == 'numeric_tolerance':
            merged = numeric_tolerance_join(
                df_a, df_b, request.left_num_key, request.right_num_key, request.tolerance_amount
            )
            # For numeric tolerance, compute diffs based on matched indices  
            if not merged.empty and '_merge' not in merged.columns:
                # Find unmatched rows by comparing indices
                matched_a_indices = merged.index.tolist() if hasattr(merged, 'index') else []
                matched_b_indices = []  # merge_asof doesn't preserve right indices easily
                
                # Simple approach: mark all as matched for now, improve later if needed
                unmatched_a = df_a.drop(matched_a_indices) if matched_a_indices else df_a
                unmatched_b = pd.DataFrame()  # merge_asof makes this complex to compute exactly
                
                diffs_list = []
                if not unmatched_a.empty:
                    unmatched_a_copy = unmatched_a.copy()
                    unmatched_a_copy['source'] = 'only_in_a'
                    diffs_list.append(unmatched_a_copy)
                
                diffs = pd.concat(diffs_list, ignore_index=True, sort=False) if diffs_list else pd.DataFrame()
            else:
                diffs = pd.DataFrame()
        
        end_time = time.time()
        duration_ms = int((end_time - start_time) * 1000)
        
        # Create QA log entry
        only_in_a_count = len(diffs[diffs['source'] == 'only_in_a']) if 'source' in diffs.columns else 0
        only_in_b_count = len(diffs[diffs['source'] == 'only_in_b']) if 'source' in diffs.columns else 0
        
        qa_log_row = {
            'timestamp': datetime.now().isoformat(),
            'join_mode': request.join_mode,
            'threshold': request.threshold if request.join_mode == 'fuzzy' else None,
            'tolerance_days': request.tolerance_days if request.join_mode == 'date_tolerance' else None,
            'tolerance_amount': request.tolerance_amount if request.join_mode == 'numeric_tolerance' else None,
            'rows_a': len(df_a),
            'rows_b': len(df_b),
            'matches_count': len(merged),
            'only_in_a_count': only_in_a_count,
            'only_in_b_count': only_in_b_count,
            'duration_ms': duration_ms
        }
        
        # Add date/numeric difference statistics if available
        if request.join_mode == 'date_tolerance' and not merged.empty and 'date_diff_days' in merged.columns:
            date_diffs = merged['date_diff_days'].dropna()
            if not date_diffs.empty:
                qa_log_row.update({
                    'date_diff_min': int(date_diffs.min()),
                    'date_diff_max': int(date_diffs.max()),
                    'date_diff_mean': float(date_diffs.mean())
                })
        
        if request.join_mode == 'numeric_tolerance' and not merged.empty and 'num_diff' in merged.columns:
            num_diffs = merged['num_diff'].dropna()
            if not num_diffs.empty:
                qa_log_row.update({
                    'num_diff_min': float(num_diffs.min()),
                    'num_diff_max': float(num_diffs.max()),
                    'num_diff_mean': float(num_diffs.mean())
                })
        
        # Write output files
        write_outputs(session_dir, merged, diffs, qa_log_row)
        
        return {
            "ready_for_download": True,
            "timestamp": qa_log_row['timestamp'],
            "join_mode": qa_log_row['join_mode'],
            "threshold": qa_log_row['threshold'],
            "rows_a": qa_log_row['rows_a'],
            "rows_b": qa_log_row['rows_b'],
            "matches_count": qa_log_row['matches_count'],
            "only_in_a_count": qa_log_row['only_in_a_count'],
            "only_in_b_count": qa_log_row['only_in_b_count'],
            "duration_ms": qa_log_row['duration_ms']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reconciliation failed: {str(e)}")


@api_router.get("/download/{file_type}")
async def download_file(file_type: str, session_id: str = Query(...)):
    """Download reconciliation results"""
    
    if file_type not in ['merged', 'diffs', 'qa_log']:
        raise HTTPException(status_code=400, detail="file_type must be one of: merged, diffs, qa_log")
    
    session_dir = SESSION_DIR / session_id
    if not session_dir.exists():
        raise HTTPException(status_code=404, detail="Session not found")
    
    file_path = session_dir / f"{file_type}.csv"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"{file_type}.csv not found. Run /reconcile first.")
    
    def iter_file():
        with open(file_path, 'rb') as file:
            while True:
                chunk = file.read(8192)
                if not chunk:
                    break
                yield chunk
    
    return StreamingResponse(
        iter_file(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={file_type}.csv"}
    )


@api_router.get("/preview")
async def preview_files(session_id: str = Query(...), n: int = Query(default=100)):
    """Preview first N rows of both uploaded files"""
    
    session_dir = SESSION_DIR / session_id
    if not session_dir.exists():
        raise HTTPException(status_code=404, detail="Session not found")
    
    file_a_path = session_dir / "file_a.csv"
    file_b_path = session_dir / "file_b.csv"
    
    if not file_a_path.exists() or not file_b_path.exists():
        raise HTTPException(status_code=404, detail="Session files not found")
    
    try:
        df_a = load_csv_safely(file_a_path)
        df_b = load_csv_safely(file_b_path)
        
        # Limit preview rows
        preview_n = min(n, 1000)  # Cap at 1000 rows for performance
        
        return {
            "file_a": {
                "columns": list(df_a.columns),
                "sample_rows": df_a.head(preview_n).to_dict('records')
            },
            "file_b": {
                "columns": list(df_b.columns),
                "sample_rows": df_b.head(preview_n).to_dict('records')
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")


@app.get("/")
async def root(request: Request):
    """Serve frontend index.html or API info based on Accept header"""
    
    # Check if frontend exists
    frontend_index = frontend_dist / "index.html"
    
    # Check if request is from a browser (Accept header contains text/html)
    accept_header = request.headers.get("accept", "")
    if "text/html" in accept_header:
        # If frontend exists, serve it
        if frontend_index.exists():
            return FileResponse(str(frontend_index))
        
        # Otherwise serve the built-in web interface
        html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CSV Reconciliation Tool</title>
    <style>
        * { box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            max-width: 1200px; 
            margin: 0 auto; 
            padding: 20px; 
            line-height: 1.6; 
            color: #333; 
            background: #f5f5f5;
        }
        .hero { 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
            color: white; 
            padding: 30px; 
            border-radius: 10px; 
            margin-bottom: 30px; 
            text-align: center; 
        }
        .card { 
            background: white; 
            border: 1px solid #dee2e6; 
            border-radius: 8px; 
            padding: 25px; 
            margin-bottom: 20px; 
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .btn { 
            background: #007bff; 
            color: white; 
            padding: 12px 24px; 
            border: none;
            border-radius: 6px; 
            cursor: pointer;
            font-size: 16px;
            margin: 5px;
        }
        .btn:hover { background: #0056b3; }
        .btn:disabled { background: #6c757d; cursor: not-allowed; }
        .btn-success { background: #28a745; }
        .btn-success:hover { background: #1e7e34; }
        .file-upload { 
            border: 2px dashed #007bff; 
            border-radius: 8px; 
            padding: 40px; 
            text-align: center; 
            margin: 15px 0;
            background: #f8f9ff;
        }
        .file-upload.dragover { 
            border-color: #0056b3; 
            background: #e3f2fd; 
        }
        .form-group { 
            margin: 15px 0; 
        }
        .form-group label { 
            display: block; 
            font-weight: bold; 
            margin-bottom: 5px; 
        }
        .form-group select, .form-group input { 
            width: 100%; 
            padding: 10px; 
            border: 1px solid #ddd; 
            border-radius: 4px; 
            font-size: 16px;
        }
        .columns-grid { 
            display: grid; 
            grid-template-columns: 1fr 1fr; 
            gap: 20px; 
            margin: 20px 0; 
        }
        .status { 
            padding: 15px; 
            border-radius: 6px; 
            margin: 15px 0; 
        }
        .status.success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .status.error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .status.info { background: #d1ecf1; color: #0c5460; border: 1px solid #bee5eb; }
        .hidden { display: none; }
        .results { 
            margin-top: 30px; 
        }
        .download-links a { 
            display: inline-block; 
            margin: 10px 10px 10px 0; 
        }
        .step { 
            background: #fff3cd; 
            padding: 15px; 
            border-left: 4px solid #ffc107; 
            margin: 10px 0; 
        }
        .step.completed { 
            background: #d4edda; 
            border-left-color: #28a745; 
        }
        .step.current { 
            background: #d1ecf1; 
            border-left-color: #007bff; 
        }
    </style>
</head>
<body>
    <div class="hero">
        <h1>🔄 CSV Reconciliation Tool</h1>
        <p>Upload two CSV files and reconcile them with exact, fuzzy, or composite matching</p>
    </div>

    <!-- Step 1: Upload Files -->
    <div class="card">
        <h2>📁 Step 1: Upload Your CSV Files</h2>
        <div class="columns-grid">
            <div>
                <h3>File A</h3>
                <div class="file-upload" id="fileA-drop">
                    <p>📄 Drag & drop your first CSV file here</p>
                    <p>or <button type="button" onclick="document.getElementById('fileA').click()" class="btn">Choose File</button></p>
                    <input type="file" id="fileA" accept=".csv,.csv.gz" style="display: none;">
                    <div id="fileA-info" class="hidden"></div>
                </div>
            </div>
            <div>
                <h3>File B</h3>
                <div class="file-upload" id="fileB-drop">
                    <p>📄 Drag & drop your second CSV file here</p>
                    <p>or <button type="button" onclick="document.getElementById('fileB').click()" class="btn">Choose File</button></p>
                    <input type="file" id="fileB" accept=".csv,.csv.gz" style="display: none;">
                    <div id="fileB-info" class="hidden"></div>
                </div>
            </div>
        </div>
        <button id="upload-btn" class="btn btn-success" disabled onclick="uploadFiles()">📤 Upload Files</button>
    </div>

    <!-- Step 2: Configure Join -->
    <div class="card hidden" id="config-section">
        <h2>⚙️ Step 2: Configure Reconciliation</h2>
        
        <div class="form-group">
            <label for="join-mode">Join Type:</label>
            <select id="join-mode" onchange="updateJoinConfig()">
                <option value="exact">🎯 Exact Match - Standard merge on specified columns</option>
                <option value="fuzzy">🔍 Fuzzy Match - Similar text matching with threshold</option>
                <option value="composite">🔗 Composite Match - Match on multiple column pairs</option>
                <option value="exact_ci">📝 Case-Insensitive - Exact match ignoring case and whitespace</option>
                <option value="phonetic">🔊 Phonetic Match - Match names that sound similar (Metaphone)</option>
                <option value="date_tolerance">📅 Date Tolerance - Match dates within ± N days</option>
                <option value="numeric_tolerance">🔢 Numeric Tolerance - Match numbers within ± X amount</option>
            </select>
        </div>

        <div class="columns-grid">
            <div class="form-group">
                <label for="left-key">Column from File A:</label>
                <select id="left-key"></select>
            </div>
            <div class="form-group">
                <label for="right-key">Column from File B:</label>
                <select id="right-key"></select>
            </div>
        </div>

        <div class="columns-grid hidden" id="composite-keys">
            <div class="form-group">
                <label for="left-key2">Second Column from File A:</label>
                <select id="left-key2"></select>
            </div>
            <div class="form-group">
                <label for="right-key2">Second Column from File B:</label>
                <select id="right-key2"></select>
            </div>
        </div>

        <div class="form-group hidden" id="threshold-section">
            <label for="threshold">Similarity Threshold (0-100):</label>
            <input type="number" id="threshold" value="90" min="0" max="100">
            <small>Higher values require closer matches</small>
        </div>

        <div class="columns-grid hidden" id="date-keys">
            <div class="form-group">
                <label for="left-date-key">Date Column from File A:</label>
                <select id="left-date-key"></select>
            </div>
            <div class="form-group">
                <label for="right-date-key">Date Column from File B:</label>
                <select id="right-date-key"></select>
            </div>
        </div>

        <div class="form-group hidden" id="date-tolerance-section">
            <label for="tolerance-days">Date Tolerance (± days):</label>
            <input type="number" id="tolerance-days" value="3" min="0">
            <small>Allow dates to differ by up to this many days</small>
        </div>

        <div class="columns-grid hidden" id="numeric-keys">
            <div class="form-group">
                <label for="left-num-key">Numeric Column from File A:</label>
                <select id="left-num-key"></select>
            </div>
            <div class="form-group">
                <label for="right-num-key">Numeric Column from File B:</label>
                <select id="right-num-key"></select>
            </div>
        </div>

        <div class="form-group hidden" id="numeric-tolerance-section">
            <label for="tolerance-amount">Numeric Tolerance (± amount):</label>
            <input type="number" id="tolerance-amount" value="1.0" step="0.1" min="0">
            <small>Allow numbers to differ by up to this amount</small>
        </div>

        <div class="columns-grid hidden" id="secondary-keys">
            <div class="form-group">
                <label for="left-secondary-key">Optional Secondary Column from File A:</label>
                <select id="left-secondary-key">
                    <option value="">-- Optional --</option>
                </select>
            </div>
            <div class="form-group">
                <label for="right-secondary-key">Optional Secondary Column from File B:</label>
                <select id="right-secondary-key">
                    <option value="">-- Optional --</option>
                </select>
            </div>
        </div>

        <button id="reconcile-btn" class="btn btn-success" onclick="reconcileFiles()">🔄 Reconcile Files</button>
    </div>

    <!-- Step 3: Results -->
    <div class="card hidden" id="results-section">
        <h2>📊 Step 3: Download Results</h2>
        <div id="reconcile-summary"></div>
        <div class="download-links">
            <a href="#" id="download-merged" class="btn">📥 Download Matched Records</a>
            <a href="#" id="download-diffs" class="btn">📥 Download Unmatched Records</a>
            <a href="#" id="download-log" class="btn">📥 Download Summary Log</a>
        </div>
        <button class="btn" onclick="startOver()">🔄 Start Over</button>
    </div>

    <!-- Status Messages -->
    <div id="status-messages"></div>

    <script>
        let sessionId = null;
        let fileAColumns = [];
        let fileBColumns = [];

        // File upload handling
        document.getElementById('fileA').addEventListener('change', function(e) {
            handleFileSelect(e.target.files[0], 'A');
        });
        
        document.getElementById('fileB').addEventListener('change', function(e) {
            handleFileSelect(e.target.files[0], 'B');
        });

        // Drag and drop
        ['fileA-drop', 'fileB-drop'].forEach(id => {
            const elem = document.getElementById(id);
            elem.addEventListener('dragover', function(e) {
                e.preventDefault();
                this.classList.add('dragover');
            });
            elem.addEventListener('dragleave', function(e) {
                e.preventDefault();
                this.classList.remove('dragover');
            });
            elem.addEventListener('drop', function(e) {
                e.preventDefault();
                this.classList.remove('dragover');
                const files = e.dataTransfer.files;
                if (files.length > 0) {
                    const fileType = id.includes('fileA') ? 'A' : 'B';
                    const inputId = 'file' + fileType;
                    document.getElementById(inputId).files = files;
                    handleFileSelect(files[0], fileType);
                }
            });
        });

        function handleFileSelect(file, type) {
            if (file && (file.name.endsWith('.csv') || file.name.endsWith('.csv.gz'))) {
                const infoDiv = document.getElementById('file' + type + '-info');
                infoDiv.className = 'status success';
                infoDiv.innerHTML = `✅ ${file.name} (${(file.size / 1024).toFixed(1)} KB)`;
                checkUploadReady();
            } else {
                showStatus('Please select a valid CSV file (.csv or .csv.gz)', 'error');
            }
        }

        function checkUploadReady() {
            const fileA = document.getElementById('fileA').files[0];
            const fileB = document.getElementById('fileB').files[0];
            document.getElementById('upload-btn').disabled = !(fileA && fileB);
        }

        async function uploadFiles() {
            const fileA = document.getElementById('fileA').files[0];
            const fileB = document.getElementById('fileB').files[0];
            
            if (!fileA || !fileB) return;

            const formData = new FormData();
            formData.append('file_a', fileA);
            formData.append('file_b', fileB);

            showStatus('⏳ Uploading files...', 'info');
            
            try {
                const response = await fetch('/api/upload', {
                    method: 'POST',
                    body: formData
                });
                
                const data = await response.json();
                
                if (response.ok) {
                    sessionId = data.session_id;
                    fileAColumns = data.file_a_columns;
                    fileBColumns = data.file_b_columns;
                    
                    showStatus(`✅ Files uploaded successfully! File A: ${data.rows_a} rows, File B: ${data.rows_b} rows`, 'success');
                    
                    populateColumnDropdowns();
                    document.getElementById('config-section').classList.remove('hidden');
                    document.getElementById('upload-btn').style.display = 'none';
                } else {
                    showStatus(`❌ Upload failed: ${data.detail}`, 'error');
                }
            } catch (error) {
                showStatus(`❌ Upload error: ${error.message}`, 'error');
            }
        }

        function populateColumnDropdowns() {
            const leftSelect = document.getElementById('left-key');
            const rightSelect = document.getElementById('right-key');
            const leftSelect2 = document.getElementById('left-key2');
            const rightSelect2 = document.getElementById('right-key2');
            const leftDateSelect = document.getElementById('left-date-key');
            const rightDateSelect = document.getElementById('right-date-key');
            const leftNumSelect = document.getElementById('left-num-key');
            const rightNumSelect = document.getElementById('right-num-key');
            const leftSecondarySelect = document.getElementById('left-secondary-key');
            const rightSecondarySelect = document.getElementById('right-secondary-key');
            
            // Populate File A dropdowns
            [leftSelect, leftSelect2, leftDateSelect, leftNumSelect].forEach(select => {
                select.innerHTML = '';
                fileAColumns.forEach(col => {
                    select.innerHTML += `<option value="${col}">${col}</option>`;
                });
            });
            
            // Populate File B dropdowns
            [rightSelect, rightSelect2, rightDateSelect, rightNumSelect].forEach(select => {
                select.innerHTML = '';
                fileBColumns.forEach(col => {
                    select.innerHTML += `<option value="${col}">${col}</option>`;
                });
            });
            
            // Populate secondary key dropdowns with optional first option
            leftSecondarySelect.innerHTML = '<option value="">-- Optional --</option>';
            rightSecondarySelect.innerHTML = '<option value="">-- Optional --</option>';
            fileAColumns.forEach(col => {
                leftSecondarySelect.innerHTML += `<option value="${col}">${col}</option>`;
            });
            fileBColumns.forEach(col => {
                rightSecondarySelect.innerHTML += `<option value="${col}">${col}</option>`;
            });
        }

        function updateJoinConfig() {
            const joinMode = document.getElementById('join-mode').value;
            const compositeDiv = document.getElementById('composite-keys');
            const thresholdDiv = document.getElementById('threshold-section');
            const dateKeysDiv = document.getElementById('date-keys');
            const dateToleranceDiv = document.getElementById('date-tolerance-section');
            const numericKeysDiv = document.getElementById('numeric-keys');
            const numericToleranceDiv = document.getElementById('numeric-tolerance-section');
            const secondaryKeysDiv = document.getElementById('secondary-keys');
            
            // Hide all mode-specific sections first
            [compositeDiv, thresholdDiv, dateKeysDiv, dateToleranceDiv, 
             numericKeysDiv, numericToleranceDiv, secondaryKeysDiv].forEach(div => {
                div.classList.add('hidden');
            });
            
            // Show relevant sections based on join mode
            if (joinMode === 'composite') {
                compositeDiv.classList.remove('hidden');
            } else if (joinMode === 'fuzzy') {
                thresholdDiv.classList.remove('hidden');
            } else if (joinMode === 'date_tolerance') {
                dateKeysDiv.classList.remove('hidden');
                dateToleranceDiv.classList.remove('hidden');
                secondaryKeysDiv.classList.remove('hidden');
            } else if (joinMode === 'numeric_tolerance') {
                numericKeysDiv.classList.remove('hidden');
                numericToleranceDiv.classList.remove('hidden');
            }
        }

        async function reconcileFiles() {
            const joinMode = document.getElementById('join-mode').value;
            
            const payload = {
                session_id: sessionId,
                join_mode: joinMode,
                threshold: parseInt(document.getElementById('threshold').value) || 90
            };
            
            // Add fields based on join mode
            if (joinMode === 'composite') {
                payload.left_key = document.getElementById('left-key').value;
                payload.right_key = document.getElementById('right-key').value;
                payload.left_key2 = document.getElementById('left-key2').value;
                payload.right_key2 = document.getElementById('right-key2').value;
            } else if (joinMode === 'date_tolerance') {
                payload.left_date_key = document.getElementById('left-date-key').value;
                payload.right_date_key = document.getElementById('right-date-key').value;
                payload.tolerance_days = parseInt(document.getElementById('tolerance-days').value) || 3;
                
                const leftSecondary = document.getElementById('left-secondary-key').value;
                const rightSecondary = document.getElementById('right-secondary-key').value;
                if (leftSecondary && rightSecondary) {
                    payload.left_secondary_key = leftSecondary;
                    payload.right_secondary_key = rightSecondary;
                }
            } else if (joinMode === 'numeric_tolerance') {
                payload.left_num_key = document.getElementById('left-num-key').value;
                payload.right_num_key = document.getElementById('right-num-key').value;
                payload.tolerance_amount = parseFloat(document.getElementById('tolerance-amount').value) || 1.0;
            } else {
                // For exact, fuzzy, exact_ci, phonetic
                payload.left_key = document.getElementById('left-key').value;
                payload.right_key = document.getElementById('right-key').value;
            }
            
            showStatus('⏳ Processing reconciliation...', 'info');
            
            try {
                const response = await fetch('/api/reconcile', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                
                const data = await response.json();
                
                if (response.ok) {
                    showStatus('✅ Reconciliation completed!', 'success');
                    displayResults(data);
                } else {
                    showStatus(`❌ Reconciliation failed: ${data.detail}`, 'error');
                }
            } catch (error) {
                showStatus(`❌ Reconciliation error: ${error.message}`, 'error');
            }
        }

        function displayResults(data) {
            const summary = document.getElementById('reconcile-summary');
            summary.innerHTML = `
                <div class="status success">
                    <h3>📊 Reconciliation Summary</h3>
                    <p><strong>Join Mode:</strong> ${data.join_mode}</p>
                    <p><strong>Matched Records:</strong> ${data.matches_count}</p>
                    <p><strong>Only in File A:</strong> ${data.only_in_a_count}</p>
                    <p><strong>Only in File B:</strong> ${data.only_in_b_count}</p>
                    <p><strong>Processing Time:</strong> ${data.duration_ms}ms</p>
                </div>
            `;
            
            // Set up download links
            document.getElementById('download-merged').href = `/api/download/merged?session_id=${sessionId}`;
            document.getElementById('download-diffs').href = `/api/download/diffs?session_id=${sessionId}`;
            document.getElementById('download-log').href = `/api/download/qa_log?session_id=${sessionId}`;
            
            document.getElementById('results-section').classList.remove('hidden');
            document.getElementById('reconcile-btn').style.display = 'none';
        }

        function showStatus(message, type) {
            const statusDiv = document.getElementById('status-messages');
            statusDiv.innerHTML = `<div class="status ${type}">${message}</div>`;
        }

        function startOver() {
            location.reload();
        }

        // Initialize
        updateJoinConfig();
    </script>

    <footer style="text-align: center; margin-top: 40px; color: #666;">
        <p>CSV Reconciliation Tool v1.0.0 | <a href="/docs">API Documentation</a></p>
    </footer>
</body>
</html>
        """
        return HTMLResponse(content=html_content)
    
    # Return JSON for API clients
    return {
        "name": "CSV Reconciliation API",
        "version": "1.0.0",
        "description": "A production-lean FastAPI app for reconciling CSVs with exact/fuzzy/composite joins",
        "endpoints": {
            "documentation": "/docs",
            "health": "/health",
            "upload": "POST /api/upload",
            "reconcile": "POST /api/reconcile", 
            "download": "GET /api/download/{file_type}?session_id=...",
            "preview": "GET /api/preview?session_id=..."
        },
        "workflow": [
            "1. POST /api/upload with two CSV files",
            "2. POST /api/reconcile with join configuration", 
            "3. GET /api/download/{merged|diffs|qa_log} for results"
        ],
        "sample_usage": {
            "interactive_docs": "Visit /docs for interactive API testing",
            "sample_data": "Use sample_data/customers_a.csv and customers_b.csv for testing"
        }
    }


# Include API router
app.include_router(api_router)

# SPA fallback - serve index.html for any unmatched routes (must be last)
@app.get("/{full_path:path}")
async def spa_fallback(full_path: str):
    """Serve index.html for SPA routes or 404 for API routes"""
    frontend_index = frontend_dist / "index.html"
    
    # If this looks like an API route, return 404
    if full_path.startswith("api/") or full_path.startswith("docs") or full_path.startswith("openapi.json"):
        raise HTTPException(status_code=404, detail="Not Found")
    
    # If frontend exists, serve index.html for SPA routing
    if frontend_index.exists():
        return FileResponse(str(frontend_index))
    
    # Otherwise return 404
    raise HTTPException(status_code=404, detail="Not Found")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 