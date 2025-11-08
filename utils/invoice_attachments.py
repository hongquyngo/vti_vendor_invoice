# utils/invoice_attachments.py

import pandas as pd
from sqlalchemy import text
import logging
import re
import time
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
from .db import get_db_engine

logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS
# ============================================================================

ALLOWED_FILE_TYPES = ['pdf', 'png', 'jpg', 'jpeg']
MAX_FILE_SIZE_MB = 10
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
MAX_FILES_COUNT = 10
S3_FOLDER_PREFIX = "purchase-invoice-file/"

# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_uploaded_files(files) -> Tuple[bool, List[str], List[Dict]]:
    """
    Validate uploaded files for type, size, and count
    
    Args:
        files: List of Streamlit UploadedFile objects
        
    Returns:
        Tuple of (is_valid, error_messages, validated_metadata)
    """
    if not files:
        return True, [], []
    
    errors = []
    metadata = []
    
    # Check file count
    if len(files) > MAX_FILES_COUNT:
        errors.append(f"❌ Too many files. Maximum {MAX_FILES_COUNT} files allowed (you selected {len(files)})")
        return False, errors, []
    
    total_size = 0
    seen_names = set()
    
    for idx, file in enumerate(files, 1):
        file_errors = []
        
        # Get file info
        filename = file.name
        file_size = file.size
        file_type = filename.split('.')[-1].lower() if '.' in filename else ''
        
        # Validate file type
        if file_type not in ALLOWED_FILE_TYPES:
            file_errors.append(f"Invalid type '.{file_type}' (allowed: {', '.join(ALLOWED_FILE_TYPES)})")
        
        # Validate file size
        if file_size > MAX_FILE_SIZE_BYTES:
            size_mb = file_size / 1024 / 1024
            file_errors.append(f"File too large ({size_mb:.1f} MB > {MAX_FILE_SIZE_MB} MB limit)")
        
        # Check for duplicate names in current upload
        if filename in seen_names:
            file_errors.append(f"Duplicate filename in upload")
        seen_names.add(filename)
        
        # Validate filename
        if not is_valid_filename(filename):
            file_errors.append(f"Invalid filename (contains special characters)")
        
        if file_errors:
            errors.append(f"File #{idx} ({filename}): {', '.join(file_errors)}")
        else:
            # Add to metadata
            metadata.append({
                'index': idx,
                'filename': filename,
                'size': file_size,
                'size_mb': round(file_size / 1024 / 1024, 2),
                'type': file_type.upper(),
                'file_object': file
            })
            total_size += file_size
    
    # Check total size (optional limit)
    total_size_mb = total_size / 1024 / 1024
    if total_size_mb > MAX_FILE_SIZE_MB * MAX_FILES_COUNT:  # Max total: 100MB
        errors.append(f"❌ Total size too large ({total_size_mb:.1f} MB)")
    
    is_valid = len(errors) == 0
    
    return is_valid, errors, metadata

def is_valid_filename(filename: str) -> bool:
    """
    Check if filename contains only safe characters
    
    Args:
        filename: Original filename
        
    Returns:
        True if valid, False otherwise
    """
    # Allow alphanumeric, spaces, underscores, hyphens, dots
    pattern = r'^[a-zA-Z0-9\s_\-\.]+$'
    return bool(re.match(pattern, filename))

# ============================================================================
# FILE PROCESSING
# ============================================================================

def prepare_files_for_upload(files, invoice_number: str = None) -> List[Dict[str, Any]]:
    """
    Prepare files for S3 upload with sanitized names and metadata
    
    Args:
        files: List of UploadedFile objects
        invoice_number: Optional invoice number to include in filename
        
    Returns:
        List of file dictionaries ready for upload
    """
    prepared_files = []
    timestamp_base = int(time.time() * 1000)  # Milliseconds
    
    for idx, file in enumerate(files):
        # Read file content
        file.seek(0)  # Reset file pointer
        file_content = file.read()
        
        # Generate unique S3 key
        timestamp = timestamp_base + idx  # Ensure uniqueness
        sanitized_name = sanitize_filename(file.name)
        s3_key = generate_s3_key(sanitized_name, timestamp)
        
        prepared_files.append({
            'original_name': file.name,
            'sanitized_name': sanitized_name,
            's3_key': s3_key,
            'content': file_content,
            'size': file.size,
            'content_type': get_content_type(file.name)
        })
    
    return prepared_files

def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename for S3 storage
    
    Args:
        filename: Original filename
        
    Returns:
        Sanitized filename
    """
    # Replace spaces with underscores
    sanitized = filename.replace(' ', '_')
    
    # Remove special characters except dots, underscores, hyphens
    sanitized = re.sub(r'[^a-zA-Z0-9._\-]', '', sanitized)
    
    # Convert to lowercase
    sanitized = sanitized.lower()
    
    # Limit length
    name_parts = sanitized.rsplit('.', 1)
    if len(name_parts) == 2:
        name, ext = name_parts
        if len(name) > 100:
            name = name[:100]
        sanitized = f"{name}.{ext}"
    
    return sanitized

def generate_s3_key(filename: str, timestamp: int = None) -> str:
    """
    Generate S3 key with timestamp prefix
    
    Args:
        filename: Sanitized filename
        timestamp: Optional timestamp (milliseconds), defaults to current time
        
    Returns:
        Full S3 key path
    """
    if timestamp is None:
        timestamp = int(time.time() * 1000)
    
    return f"{S3_FOLDER_PREFIX}{timestamp}_{filename}"

def get_content_type(filename: str) -> str:
    """
    Get MIME content type from filename
    
    Args:
        filename: File name
        
    Returns:
        MIME type string
    """
    extension = filename.split('.')[-1].lower() if '.' in filename else ''
    
    content_types = {
        'pdf': 'application/pdf',
        'png': 'image/png',
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg'
    }
    
    return content_types.get(extension, 'application/octet-stream')

# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

def save_media_records(s3_keys: List[str], keycloak_id: str) -> Tuple[bool, List[int], str]:
    """
    Save media records to database
    
    Args:
        s3_keys: List of S3 keys to save
        keycloak_id: User's keycloak ID
        
    Returns:
        Tuple of (success, media_ids, error_message)
    """
    if not s3_keys:
        return True, [], ""
    
    media_ids = []
    engine = get_db_engine()
    
    try:
        with engine.begin() as conn:
            for s3_key in s3_keys:
                # Extract filename from S3 key
                filename = s3_key.split('/')[-1]
                
                query = text("""
                INSERT INTO medias (
                    created_by,
                    created_date,
                    name,
                    path,
                    updated_date,
                    version
                ) VALUES (
                    :created_by,
                    NOW(),
                    :name,
                    :path,
                    NOW(),
                    0
                )
                """)
                
                result = conn.execute(query, {
                    'created_by': keycloak_id,
                    'name': filename,
                    'path': s3_key
                })
                
                media_id = result.lastrowid
                media_ids.append(media_id)
                
                logger.info(f"Created media record {media_id} for {filename}")
        
        return True, media_ids, ""
        
    except Exception as e:
        error_msg = f"Failed to save media records: {str(e)}"
        logger.error(error_msg)
        return False, [], error_msg

def link_media_to_invoice(invoice_id: int, media_ids: List[int], keycloak_id: str) -> Tuple[bool, str]:
    """
    Link media files to invoice in purchase_invoice_medias table
    
    Args:
        invoice_id: Purchase invoice ID
        media_ids: List of media IDs to link
        keycloak_id: User's keycloak ID
        
    Returns:
        Tuple of (success, error_message)
    """
    if not media_ids:
        return True, ""
    
    engine = get_db_engine()
    
    try:
        with engine.begin() as conn:
            for media_id in media_ids:
                query = text("""
                INSERT INTO purchase_invoice_medias (
                    purchase_invoice_id,
                    media_id,
                    created_by,
                    created_date,
                    delete_flag,
                    version
                ) VALUES (
                    :purchase_invoice_id,
                    :media_id,
                    :created_by,
                    NOW(),
                    0,
                    0
                )
                """)
                
                conn.execute(query, {
                    'purchase_invoice_id': invoice_id,
                    'media_id': media_id,
                    'created_by': keycloak_id
                })
                
                logger.info(f"Linked media {media_id} to invoice {invoice_id}")
        
        return True, ""
        
    except Exception as e:
        error_msg = f"Failed to link media to invoice: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

# ============================================================================
# QUERY OPERATIONS
# ============================================================================

def get_invoice_attachments(invoice_id: int) -> pd.DataFrame:
    """
    Get all attachments for an invoice
    
    Args:
        invoice_id: Purchase invoice ID
        
    Returns:
        DataFrame with attachment information
    """
    try:
        engine = get_db_engine()
        
        query = text("""
        SELECT 
            m.id as media_id,
            m.name as filename,
            m.path as s3_key,
            m.created_by,
            m.created_date,
            pim.id as link_id,
            CONCAT(e.first_name, ' ', e.last_name) as uploaded_by
        FROM purchase_invoice_medias pim
        JOIN medias m ON pim.media_id = m.id
        LEFT JOIN employees e ON m.created_by = e.keycloak_id
        WHERE pim.purchase_invoice_id = :invoice_id
            AND pim.delete_flag = 0
        ORDER BY m.created_date DESC
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'invoice_id': invoice_id})
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting invoice attachments: {e}")
        return pd.DataFrame()

def delete_invoice_attachment(link_id: int, keycloak_id: str) -> Tuple[bool, str]:
    """
    Soft delete an invoice attachment link
    
    Args:
        link_id: purchase_invoice_medias ID
        keycloak_id: User's keycloak ID
        
    Returns:
        Tuple of (success, message)
    """
    try:
        engine = get_db_engine()
        
        query = text("""
        UPDATE purchase_invoice_medias
        SET delete_flag = 1,
            modified_date = NOW()
        WHERE id = :link_id
            AND delete_flag = 0
        """)
        
        with engine.begin() as conn:
            result = conn.execute(query, {'link_id': link_id})
            
            if result.rowcount > 0:
                logger.info(f"Deleted attachment link {link_id}")
                return True, "Attachment removed successfully"
            else:
                return False, "Attachment not found or already deleted"
                
    except Exception as e:
        error_msg = f"Error deleting attachment: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

# ============================================================================
# CLEANUP FUNCTIONS
# ============================================================================

def cleanup_failed_uploads(s3_keys: List[str], s3_manager) -> None:
    """
    Delete S3 files that were uploaded but transaction failed
    
    Args:
        s3_keys: List of S3 keys to delete
        s3_manager: S3Manager instance
    """
    if not s3_keys:
        return
    
    logger.warning(f"Cleaning up {len(s3_keys)} failed uploads from S3")
    
    for s3_key in s3_keys:
        try:
            s3_manager.delete_file(s3_key)
            logger.info(f"Deleted failed upload: {s3_key}")
        except Exception as e:
            logger.error(f"Failed to delete {s3_key}: {e}")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format
    
    Args:
        size_bytes: File size in bytes
        
    Returns:
        Formatted string (e.g., "2.5 MB")
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"

def get_file_icon(filename: str) -> str:
    """
    Get emoji icon based on file type
    
    Args:
        filename: File name
        
    Returns:
        Emoji icon
    """
    extension = filename.split('.')[-1].lower() if '.' in filename else ''
    
    icons = {
        'pdf': '📄',
        'png': '🖼️',
        'jpg': '🖼️',
        'jpeg': '🖼️'
    }
    
    return icons.get(extension, '📎')

def summarize_files(metadata: List[Dict]) -> Dict[str, Any]:
    """
    Create summary statistics for uploaded files
    
    Args:
        metadata: List of file metadata dictionaries
        
    Returns:
        Summary dictionary
    """
    if not metadata:
        return {
            'count': 0,
            'total_size': 0,
            'total_size_formatted': '0 B',
            'types': []
        }
    
    total_size = sum(f['size'] for f in metadata)
    types = list(set(f['type'] for f in metadata))
    
    return {
        'count': len(metadata),
        'total_size': total_size,
        'total_size_formatted': format_file_size(total_size),
        'types': types
    }