import os
import pytest
import shutil
import tempfile
import uuid
from pathlib import Path
from fastapi.testclient import TestClient
from main import app
import pandas as pd
import io

client = TestClient(app)

@pytest.fixture
def sample_csv_a():
    """Sample CSV data for file A"""
    return """id,name,email,city
1,Alice,alice@test.com,New York
2,Bob,bob@test.com,Boston
3,Charlie,charlie@test.com,Chicago"""

@pytest.fixture
def sample_csv_b():
    """Sample CSV data for file B"""
    return """id,name,email,city
1,Alice,alice@test.com,New York
2,Bob,bob123@test.com,Boston
4,Daniel,daniel@test.com,Denver"""

@pytest.fixture
def sample_csv_composite_a():
    """Sample CSV data for composite join testing"""
    return """first_name,last_name,email,age
Alice,Smith,alice@test.com,25
Bob,Jones,bob@test.com,30
Charlie,Brown,charlie@test.com,35"""

@pytest.fixture
def sample_csv_composite_b():
    """Sample CSV data for composite join testing"""
    return """first,last,contact,years
Alice,Smith,alice@test.com,25
Bob,Jones,bob123@test.com,30
David,Wilson,david@test.com,40"""

@pytest.fixture
def sample_csv_case_insensitive_a():
    """Sample CSV data for case-insensitive testing"""
    return """id,name,email
1, Alice ,alice@test.com
2,BOB,bob@test.com
3,Charlie  ,charlie@test.com"""

@pytest.fixture
def sample_csv_case_insensitive_b():
    """Sample CSV data for case-insensitive testing"""
    return """id,name,email
1,alice,alice@test.com
2,bob,bob123@test.com
4,DANIEL,daniel@test.com"""

@pytest.fixture
def sample_csv_phonetic_a():
    """Sample CSV data for phonetic testing"""
    return """id,name,email
1,Steven,steven@test.com
2,Katherine,kate@test.com
3,Michael,mike@test.com"""

@pytest.fixture
def sample_csv_phonetic_b():
    """Sample CSV data for phonetic testing"""
    return """id,name,email
1,Stephen,stephen@test.com
2,Catherine,cathy@test.com
4,Mitchell,mitch@test.com"""

@pytest.fixture
def sample_csv_date_a():
    """Sample CSV data for date tolerance testing"""
    return """id,name,date,amount
1,Alice,2025-01-01,100.0
2,Bob,2025-01-10,200.0
3,Charlie,2025-01-20,300.0"""

@pytest.fixture
def sample_csv_date_b():
    """Sample CSV data for date tolerance testing"""
    return """id,name,date,amount
1,Alice,2025-01-02,105.0
2,Bob,2025-01-12,210.0
4,Daniel,2025-01-25,400.0"""

@pytest.fixture
def sample_csv_numeric_a():
    """Sample CSV data for numeric tolerance testing"""
    return """id,name,amount,score
1,Alice,99.5,85.2
2,Bob,101.2,92.1
3,Charlie,200.0,78.5"""

@pytest.fixture
def sample_csv_numeric_b():
    """Sample CSV data for numeric tolerance testing"""
    return """id,name,amount,score
1,Alice,100.0,84.8
2,Bob,102.0,91.9
4,Daniel,250.0,88.3"""

@pytest.fixture
def temp_csv_files(sample_csv_a, sample_csv_b):
    """Create temporary CSV files for testing"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f_a:
        f_a.write(sample_csv_a)
        f_a.flush()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f_b:
            f_b.write(sample_csv_b)
            f_b.flush()
            
            yield f_a.name, f_b.name
    
    # Cleanup
    os.unlink(f_a.name)
    os.unlink(f_b.name)

@pytest.fixture
def uploaded_session(temp_csv_files):
    """Upload sample files and return session info"""
    file_a_path, file_b_path = temp_csv_files
    
    with open(file_a_path, 'rb') as f_a, open(file_b_path, 'rb') as f_b:
        response = client.post(
            "/upload",
            files={
                "file_a": ("customers_a.csv", f_a, "text/csv"),
                "file_b": ("customers_b.csv", f_b, "text/csv")
            }
        )
    
    assert response.status_code == 200
    return response.json()

class TestHealthCheck:
    def test_health_endpoint(self):
        """Test health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"ok": True}

class TestUpload:
    def test_upload_valid_files(self, temp_csv_files):
        """Test uploading valid CSV files"""
        file_a_path, file_b_path = temp_csv_files
        
        with open(file_a_path, 'rb') as f_a, open(file_b_path, 'rb') as f_b:
            response = client.post(
                "/upload",
                files={
                    "file_a": ("customers_a.csv", f_a, "text/csv"),
                    "file_b": ("customers_b.csv", f_b, "text/csv")
                }
            )
        
        assert response.status_code == 200
        data = response.json()
        
        assert "session_id" in data
        assert "file_a_columns" in data
        assert "file_b_columns" in data
        assert data["rows_a"] == 3
        assert data["rows_b"] == 3
        assert data["file_a_columns"] == ["id", "name", "email", "city"]
        assert data["file_b_columns"] == ["id", "name", "email", "city"]
    
    def test_upload_invalid_extension(self):
        """Test uploading files with invalid extensions"""
        content = b"not,csv,content\n1,2,3"
        
        response = client.post(
            "/upload",
            files={
                "file_a": ("test.txt", io.BytesIO(content), "text/plain"),
                "file_b": ("test.csv", io.BytesIO(content), "text/csv")
            }
        )
        
        assert response.status_code == 400
        assert "must be a CSV file" in response.json()["detail"]
    
    def test_upload_empty_csv(self):
        """Test uploading empty CSV files"""
        empty_content = b""
        valid_content = b"id,name\n1,Alice"
        
        response = client.post(
            "/upload",
            files={
                "file_a": ("empty.csv", io.BytesIO(empty_content), "text/csv"),
                "file_b": ("valid.csv", io.BytesIO(valid_content), "text/csv")
            }
        )
        
        assert response.status_code == 400
        assert "CSV file is empty" in response.json()["detail"]

class TestExactJoin:
    def test_exact_join_on_id(self, uploaded_session):
        """Test exact join on ID column"""
        session_id = uploaded_session["session_id"]
        
        response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "exact",
            "left_key": "id",
            "right_key": "id",
            "threshold": 90
        })
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["ready_for_download"] is True
        assert data["matches_count"] == 2  # Alice and Bob match on ID
        assert data["only_in_a_count"] == 1  # Charlie only in A
        assert data["only_in_b_count"] == 1  # Daniel only in B
        assert data["join_mode"] == "exact"
    
    def test_exact_join_missing_key(self, uploaded_session):
        """Test exact join with missing key column"""
        session_id = uploaded_session["session_id"]
        
        response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "exact",
            "left_key": "nonexistent_column",
            "right_key": "id",
            "threshold": 90
        })
        
        assert response.status_code == 400
        assert "not found in file A" in response.json()["detail"]

class TestFuzzyJoin:
    def test_fuzzy_join_on_name(self, uploaded_session):
        """Test fuzzy join on name column"""
        session_id = uploaded_session["session_id"]
        
        response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "fuzzy",
            "left_key": "name",
            "right_key": "name",
            "threshold": 85
        })
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["ready_for_download"] is True
        assert data["matches_count"] >= 2  # Alice and Bob should match
        assert data["join_mode"] == "fuzzy"
        assert data["threshold"] == 85
    
    def test_fuzzy_join_high_threshold(self, uploaded_session):
        """Test fuzzy join with high threshold (fewer matches)"""
        session_id = uploaded_session["session_id"]
        
        response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "fuzzy",
            "left_key": "name",
            "right_key": "name",
            "threshold": 99
        })
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["ready_for_download"] is True
        assert data["matches_count"] <= 2  # Only exact matches should pass
        assert data["join_mode"] == "fuzzy"
        assert data["threshold"] == 99
    
    def test_fuzzy_join_invalid_threshold(self, uploaded_session):
        """Test fuzzy join with invalid threshold"""
        session_id = uploaded_session["session_id"]
        
        response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "fuzzy",
            "left_key": "name",
            "right_key": "name",
            "threshold": 150  # Invalid threshold > 100
        })
        
        assert response.status_code == 422  # Validation error

class TestCompositeJoin:
    def test_composite_join_setup_and_execution(self, sample_csv_composite_a, sample_csv_composite_b):
        """Test composite join with two key pairs"""
        # Create temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f_a:
            f_a.write(sample_csv_composite_a)
            f_a.flush()
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f_b:
                f_b.write(sample_csv_composite_b)
                f_b.flush()
                
                try:
                    # Upload files
                    with open(f_a.name, 'rb') as fa, open(f_b.name, 'rb') as fb:
                        upload_response = client.post(
                            "/upload",
                            files={
                                "file_a": ("composite_a.csv", fa, "text/csv"),
                                "file_b": ("composite_b.csv", fb, "text/csv")
                            }
                        )
                    
                    assert upload_response.status_code == 200
                    session_id = upload_response.json()["session_id"]
                    
                    # Test composite join
                    response = client.post("/reconcile", json={
                        "session_id": session_id,
                        "join_mode": "composite",
                        "left_key": "first_name",
                        "left_key2": "last_name",
                        "right_key": "first",
                        "right_key2": "last",
                        "threshold": 90
                    })
                    
                    assert response.status_code == 200
                    data = response.json()
                    
                    assert data["ready_for_download"] is True
                    assert data["matches_count"] >= 1  # At least Alice Smith should match
                    assert data["join_mode"] == "composite"
                    
                finally:
                    # Cleanup
                    os.unlink(f_a.name)
                    os.unlink(f_b.name)
    
    def test_composite_join_missing_key2(self, uploaded_session):
        """Test composite join with missing second key"""
        session_id = uploaded_session["session_id"]
        
        response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "composite",
            "left_key": "id",
            "right_key": "id"
            # Missing left_key2 and right_key2
        })
        
        assert response.status_code == 422  # Validation error

class TestDownload:
    def test_download_merged_file(self, uploaded_session):
        """Test downloading merged results"""
        session_id = uploaded_session["session_id"]
        
        # First reconcile
        reconcile_response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "exact",
            "left_key": "id",
            "right_key": "id",
            "threshold": 90
        })
        assert reconcile_response.status_code == 200
        
        # Download merged file
        download_response = client.get(f"/download/merged?session_id={session_id}")
        assert download_response.status_code == 200
        assert download_response.headers["content-type"] == "text/csv; charset=utf-8"
        assert "attachment" in download_response.headers["content-disposition"]
    
    def test_download_diffs_file(self, uploaded_session):
        """Test downloading diffs results"""
        session_id = uploaded_session["session_id"]
        
        # First reconcile
        reconcile_response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "exact",
            "left_key": "id",
            "right_key": "id",
            "threshold": 90
        })
        assert reconcile_response.status_code == 200
        
        # Download diffs file
        download_response = client.get(f"/download/diffs?session_id={session_id}")
        assert download_response.status_code == 200
        assert download_response.headers["content-type"] == "text/csv; charset=utf-8"
    
    def test_download_qa_log_file(self, uploaded_session):
        """Test downloading QA log results"""
        session_id = uploaded_session["session_id"]
        
        # First reconcile
        reconcile_response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "exact",
            "left_key": "id",
            "right_key": "id",
            "threshold": 90
        })
        assert reconcile_response.status_code == 200
        
        # Download QA log file
        download_response = client.get(f"/download/qa_log?session_id={session_id}")
        assert download_response.status_code == 200
        assert download_response.headers["content-type"] == "text/csv; charset=utf-8"
    
    def test_download_invalid_file_type(self, uploaded_session):
        """Test downloading with invalid file type"""
        session_id = uploaded_session["session_id"]
        
        response = client.get(f"/download/invalid_type?session_id={session_id}")
        assert response.status_code == 400
        assert "file_type must be one of" in response.json()["detail"]
    
    def test_download_invalid_session(self):
        """Test downloading with invalid session ID"""
        response = client.get("/download/merged?session_id=invalid-session-id")
        assert response.status_code == 404
        assert "Session not found" in response.json()["detail"]
    
    def test_download_before_reconcile(self, uploaded_session):
        """Test downloading before running reconciliation"""
        session_id = uploaded_session["session_id"]
        
        response = client.get(f"/download/merged?session_id={session_id}")
        assert response.status_code == 404
        assert "merged.csv not found" in response.json()["detail"]

class TestPreview:
    def test_preview_files(self, uploaded_session):
        """Test preview endpoint"""
        session_id = uploaded_session["session_id"]
        
        response = client.get(f"/preview?session_id={session_id}&n=5")
        assert response.status_code == 200
        
        data = response.json()
        assert "file_a" in data
        assert "file_b" in data
        assert "columns" in data["file_a"]
        assert "sample_rows" in data["file_a"]
        assert "columns" in data["file_b"]
        assert "sample_rows" in data["file_b"]
        
        # Check that we get the expected columns
        assert data["file_a"]["columns"] == ["id", "name", "email", "city"]
        assert data["file_b"]["columns"] == ["id", "name", "email", "city"]
        
        # Check that we get sample rows
        assert len(data["file_a"]["sample_rows"]) <= 5
        assert len(data["file_b"]["sample_rows"]) <= 5
    
    def test_preview_invalid_session(self):
        """Test preview with invalid session ID"""
        response = client.get("/preview?session_id=invalid-session-id")
        assert response.status_code == 404
        assert "Session not found" in response.json()["detail"]

class TestValidation:
    def test_invalid_join_mode(self, uploaded_session):
        """Test invalid join mode"""
        session_id = uploaded_session["session_id"]
        
        response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "invalid_mode",
            "left_key": "id",
            "right_key": "id",
            "threshold": 90
        })
        
        assert response.status_code == 422  # Validation error
    
    def test_session_not_found(self):
        """Test reconciling with non-existent session"""
        response = client.post("/reconcile", json={
            "session_id": "non-existent-session",
            "join_mode": "exact",
            "left_key": "id",
            "right_key": "id",
            "threshold": 90
        })
        
        assert response.status_code == 404
        assert "Session not found" in response.json()["detail"]

class TestExactCaseInsensitiveJoin:
    def test_exact_ci_join_with_case_differences(self, sample_csv_case_insensitive_a, sample_csv_case_insensitive_b):
        """Test case-insensitive exact join with whitespace and case differences"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f_a:
            f_a.write(sample_csv_case_insensitive_a)
            f_a.flush()
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f_b:
                f_b.write(sample_csv_case_insensitive_b)
                f_b.flush()
                
                try:
                    # Upload files
                    with open(f_a.name, 'rb') as fa, open(f_b.name, 'rb') as fb:
                        upload_response = client.post(
                            "/upload",
                            files={
                                "file_a": ("case_a.csv", fa, "text/csv"),
                                "file_b": ("case_b.csv", fb, "text/csv")
                            }
                        )
                    
                    assert upload_response.status_code == 200
                    session_id = upload_response.json()["session_id"]
                    
                    # Test exact_ci join on name (should match despite case/whitespace)
                    response = client.post("/reconcile", json={
                        "session_id": session_id,
                        "join_mode": "exact_ci",
                        "left_key": "name",
                        "right_key": "name",
                        "threshold": 90
                    })
                    
                    assert response.status_code == 200
                    data = response.json()
                    
                    assert data["ready_for_download"] is True
                    assert data["matches_count"] >= 2  # Alice and Bob should match despite case
                    assert data["join_mode"] == "exact_ci"
                    
                finally:
                    os.unlink(f_a.name)
                    os.unlink(f_b.name)

class TestPhoneticJoin:
    def test_phonetic_join_similar_sounding_names(self, sample_csv_phonetic_a, sample_csv_phonetic_b):
        """Test phonetic join with similar sounding names"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f_a:
            f_a.write(sample_csv_phonetic_a)
            f_a.flush()
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f_b:
                f_b.write(sample_csv_phonetic_b)
                f_b.flush()
                
                try:
                    # Upload files
                    with open(f_a.name, 'rb') as fa, open(f_b.name, 'rb') as fb:
                        upload_response = client.post(
                            "/upload",
                            files={
                                "file_a": ("phonetic_a.csv", fa, "text/csv"),
                                "file_b": ("phonetic_b.csv", fb, "text/csv")
                            }
                        )
                    
                    assert upload_response.status_code == 200
                    session_id = upload_response.json()["session_id"]
                    
                    # Test phonetic join on name
                    response = client.post("/reconcile", json={
                        "session_id": session_id,
                        "join_mode": "phonetic",
                        "left_key": "name",
                        "right_key": "name",
                        "threshold": 90
                    })
                    
                    assert response.status_code == 200
                    data = response.json()
                    
                    assert data["ready_for_download"] is True
                    assert data["matches_count"] >= 2  # Steven/Stephen and Katherine/Catherine should match
                    assert data["join_mode"] == "phonetic"
                    
                    # Download merged file to check for match_key column
                    download_response = client.get(f"/download/merged?session_id={session_id}")
                    assert download_response.status_code == 200
                    content = download_response.content.decode()
                    assert "match_key" in content
                    assert "phonetic" in content
                    
                finally:
                    os.unlink(f_a.name)
                    os.unlink(f_b.name)

class TestDateToleranceJoin:
    def test_date_tolerance_join_within_range(self, sample_csv_date_a, sample_csv_date_b):
        """Test date tolerance join with dates within tolerance range"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f_a:
            f_a.write(sample_csv_date_a)
            f_a.flush()
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f_b:
                f_b.write(sample_csv_date_b)
                f_b.flush()
                
                try:
                    # Upload files
                    with open(f_a.name, 'rb') as fa, open(f_b.name, 'rb') as fb:
                        upload_response = client.post(
                            "/upload",
                            files={
                                "file_a": ("date_a.csv", fa, "text/csv"),
                                "file_b": ("date_b.csv", fb, "text/csv")
                            }
                        )
                    
                    assert upload_response.status_code == 200
                    session_id = upload_response.json()["session_id"]
                    
                    # Test date tolerance join
                    response = client.post("/reconcile", json={
                        "session_id": session_id,
                        "join_mode": "date_tolerance",
                        "left_date_key": "date",
                        "right_date_key": "date",
                        "tolerance_days": 3
                    })
                    
                    assert response.status_code == 200
                    data = response.json()
                    
                    assert data["ready_for_download"] is True
                    assert data["matches_count"] >= 1  # At least some dates should match within 3 days
                    assert data["join_mode"] == "date_tolerance"
                    
                    # Check for date difference statistics in response
                    if "date_diff_min" in data or "date_diff_max" in data:
                        assert abs(data["date_diff_min"]) <= 3
                        assert abs(data["date_diff_max"]) <= 3
                    
                    # Download merged file to check for date_diff_days column
                    download_response = client.get(f"/download/merged?session_id={session_id}")
                    assert download_response.status_code == 200
                    content = download_response.content.decode()
                    if data["matches_count"] > 0:
                        assert "date_diff_days" in content
                    
                finally:
                    os.unlink(f_a.name)
                    os.unlink(f_b.name)

    def test_date_tolerance_missing_keys_error(self, uploaded_session):
        """Test date tolerance join with missing required keys"""
        session_id = uploaded_session["session_id"]
        
        response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "date_tolerance",
            "left_key": "id",
            "right_key": "id"
            # Missing left_date_key and right_date_key
        })
        
        assert response.status_code == 422  # Validation error

class TestNumericToleranceJoin:
    def test_numeric_tolerance_join_within_range(self, sample_csv_numeric_a, sample_csv_numeric_b):
        """Test numeric tolerance join with values within tolerance range"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f_a:
            f_a.write(sample_csv_numeric_a)
            f_a.flush()
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f_b:
                f_b.write(sample_csv_numeric_b)
                f_b.flush()
                
                try:
                    # Upload files
                    with open(f_a.name, 'rb') as fa, open(f_b.name, 'rb') as fb:
                        upload_response = client.post(
                            "/upload",
                            files={
                                "file_a": ("numeric_a.csv", fa, "text/csv"),
                                "file_b": ("numeric_b.csv", fb, "text/csv")
                            }
                        )
                    
                    assert upload_response.status_code == 200
                    session_id = upload_response.json()["session_id"]
                    
                    # Test numeric tolerance join on amount
                    response = client.post("/reconcile", json={
                        "session_id": session_id,
                        "join_mode": "numeric_tolerance",
                        "left_num_key": "amount",
                        "right_num_key": "amount",
                        "tolerance_amount": 1.0
                    })
                    
                    assert response.status_code == 200
                    data = response.json()
                    
                    assert data["ready_for_download"] is True
                    assert data["matches_count"] >= 1  # Should match 99.5↔100.0 and possibly others
                    assert data["join_mode"] == "numeric_tolerance"
                    
                    # Check for numeric difference statistics in response
                    if "num_diff_min" in data or "num_diff_max" in data:
                        assert abs(data["num_diff_min"]) <= 1.0
                        assert abs(data["num_diff_max"]) <= 1.0
                    
                    # Download merged file to check for num_diff column
                    download_response = client.get(f"/download/merged?session_id={session_id}")
                    assert download_response.status_code == 200
                    content = download_response.content.decode()
                    if data["matches_count"] > 0:
                        assert "num_diff" in content
                    
                finally:
                    os.unlink(f_a.name)
                    os.unlink(f_b.name)

    def test_numeric_tolerance_missing_keys_error(self, uploaded_session):
        """Test numeric tolerance join with missing required keys"""
        session_id = uploaded_session["session_id"]
        
        response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "numeric_tolerance",
            "left_key": "id",
            "right_key": "id"
            # Missing left_num_key and right_num_key
        })
        
        assert response.status_code == 422  # Validation error

class TestNewModesValidation:
    def test_negative_tolerance_days_error(self, uploaded_session):
        """Test validation error for negative tolerance days"""
        session_id = uploaded_session["session_id"]
        
        response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "date_tolerance",
            "left_date_key": "date",
            "right_date_key": "date", 
            "tolerance_days": -1
        })
        
        assert response.status_code == 422  # Validation error

    def test_negative_tolerance_amount_error(self, uploaded_session):
        """Test validation error for negative tolerance amount"""
        session_id = uploaded_session["session_id"]
        
        response = client.post("/reconcile", json={
            "session_id": session_id,
            "join_mode": "numeric_tolerance",
            "left_num_key": "amount",
            "right_num_key": "amount",
            "tolerance_amount": -0.5
        })
        
        assert response.status_code == 422  # Validation error

if __name__ == "__main__":
    pytest.main([__file__]) 