# utils/s3_utils.py

import boto3
from botocore.exceptions import ClientError
import logging
import json
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
import os
from .config import config

# Setup logger
logger = logging.getLogger(__name__)

class S3Manager:
    """S3 Manager for handling all S3 operations"""
    
    def __init__(self):
        """Initialize S3 client with credentials from config"""
        try:
            # Get AWS config
            aws_config = config.aws_config
            
            # Validate required config
            if not all([
                aws_config.get('access_key_id'),
                aws_config.get('secret_access_key'),
                aws_config.get('region'),
                aws_config.get('bucket_name')
            ]):
                raise ValueError("Missing required AWS configuration")
            
            # Initialize S3 client
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_config['access_key_id'],
                aws_secret_access_key=aws_config['secret_access_key'],
                region_name=aws_config['region']
            )
            
            self.bucket_name = aws_config['bucket_name']
            self.app_prefix = aws_config.get('app_prefix', 'streamlit-app')
            
            logger.info(f"✅ S3Manager initialized for bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize S3Manager: {e}")
            raise
    
    # ==================== Basic S3 Operations ====================
    
    def list_files(self, prefix: str = '', max_keys: int = 1000) -> List[Dict]:
        """
        List files in S3 bucket with optional prefix filter
        
        Args:
            prefix: S3 prefix to filter files
            max_keys: Maximum number of files to return
            
        Returns:
            List of file dictionaries with metadata
        """
        try:
            # Ensure prefix ends with / if provided
            if prefix and not prefix.endswith('/'):
                prefix += '/'
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Skip directory markers and .keep files
                    if obj['Key'].endswith('/') or obj['Key'].endswith('.keep'):
                        continue
                        
                    files.append({
                        'key': obj['Key'],
                        'name': obj['Key'].split('/')[-1],
                        'size': obj['Size'],
                        'size_mb': round(obj['Size'] / 1024 / 1024, 2),
                        'last_modified': obj['LastModified'],
                        'etag': obj.get('ETag', '').strip('"')
                    })
            
            logger.info(f"Listed {len(files)} files with prefix: {prefix}")
            return files
            
        except ClientError as e:
            logger.error(f"Error listing files: {e}")
            return []
    
    def get_folders(self, prefix: str = '') -> List[str]:
        """
        Get list of folders (common prefixes) in bucket
        
        Args:
            prefix: Parent prefix to search within
            
        Returns:
            List of folder names
        """
        try:
            # Ensure prefix ends with / if provided
            if prefix and not prefix.endswith('/'):
                prefix += '/'
                
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )
            
            folders = []
            if 'CommonPrefixes' in response:
                for prefix_info in response['CommonPrefixes']:
                    folder_path = prefix_info['Prefix']
                    folder_name = folder_path.rstrip('/').split('/')[-1]
                    folders.append(folder_name)
            
            return sorted(folders)
            
        except ClientError as e:
            logger.error(f"Error getting folders: {e}")
            return []
    
    def upload_file(self, file_content: bytes, key: str, content_type: str = None) -> Tuple[bool, str]:
        """
        Upload file to S3
        
        Args:
            file_content: File content as bytes
            key: S3 key (path) for the file
            content_type: MIME type of the file
            
        Returns:
            Tuple of (success: bool, result: str)
        """
        try:
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=file_content,
                **extra_args
            )
            
            logger.info(f"Successfully uploaded file to: {key}")
            return True, key
            
        except ClientError as e:
            error_msg = f"Failed to upload file: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def download_file(self, key: str) -> Optional[bytes]:
        """
        Download file content from S3
        
        Args:
            key: S3 key of the file
            
        Returns:
            File content as bytes or None if error
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            content = response['Body'].read()
            logger.info(f"Successfully downloaded file: {key}")
            return content
            
        except ClientError as e:
            logger.error(f"Error downloading file {key}: {e}")
            return None
    
    def delete_file(self, key: str) -> bool:
        """
        Delete file from S3
        
        Args:
            key: S3 key of the file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            logger.info(f"Successfully deleted file: {key}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting file {key}: {e}")
            return False
    
    def get_presigned_url(self, key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate presigned URL for file access
        
        Args:
            key: S3 key of the file
            expiration: URL expiration time in seconds (default 1 hour)
            
        Returns:
            Presigned URL or None if error
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': key
                },
                ExpiresIn=expiration
            )
            return url
            
        except ClientError as e:
            logger.error(f"Error generating presigned URL for {key}: {e}")
            return None
    
    def get_file_info(self, key: str) -> Optional[Dict]:
        """
        Get detailed file information
        
        Args:
            key: S3 key of the file
            
        Returns:
            Dictionary with file metadata or None if error
        """
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            return {
                'size': response['ContentLength'],
                'size_mb': round(response['ContentLength'] / 1024 / 1024, 2),
                'content_type': response.get('ContentType', 'unknown'),
                'last_modified': response['LastModified'],
                'etag': response.get('ETag', '').strip('"'),
                'metadata': response.get('Metadata', {})
            }
            
        except ClientError as e:
            logger.error(f"Error getting file info for {key}: {e}")
            return None
    
    def file_exists(self, key: str) -> bool:
        """
        Check if file exists in S3
        
        Args:
            key: S3 key to check
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )
            return True
        except ClientError:
            return False
    
    # ==================== Label Management Specific Methods ====================
    
    def create_label_folders(self):
        """Create initial folder structure for label management"""
        folders = [
            f'{self.app_prefix}/label-management/',
            f'{self.app_prefix}/label-management/customer-requirements/',
            f'{self.app_prefix}/label-management/templates/',
            f'{self.app_prefix}/label-management/assets/',
            f'{self.app_prefix}/label-management/assets/logos/',
            f'{self.app_prefix}/label-management/assets/icons/',
            f'{self.app_prefix}/label-management/assets/fonts/',
            f'{self.app_prefix}/label-management/samples/'
        ]
        
        created_count = 0
        for folder in folders:
            try:
                # Create a placeholder file to make folder visible
                placeholder_key = f"{folder}.keep"
                
                # Check if placeholder already exists
                if not self.file_exists(placeholder_key):
                    # Create placeholder file
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=placeholder_key,
                        Body=b'# This file keeps the folder structure',
                        ContentType='text/plain'
                    )
                    logger.info(f"Created folder: {folder}")
                    created_count += 1
                else:
                    logger.info(f"Folder already exists: {folder}")
                    
            except Exception as e:
                logger.error(f"Error creating folder {folder}: {e}")
        
        logger.info(f"Label folder setup complete. Created {created_count} new folders.")
        return created_count
    
    def list_customer_files(self, customer_id: int) -> List[Dict]:
        """
        List all files for a specific customer
        
        Args:
            customer_id: Customer ID
            
        Returns:
            List of file dictionaries
        """
        prefix = f"{self.app_prefix}/label-management/customer-requirements/{customer_id}/"
        return self.list_files(prefix=prefix)
    
    def upload_label_requirement(self, file_content: bytes, filename: str, 
                               customer_id: int, file_type: str = 'requirement') -> Tuple[bool, str]:
        """
        Upload label requirement file for a customer
        
        Args:
            file_content: File content as bytes
            filename: Original filename
            customer_id: Customer ID
            file_type: Type of file (requirement, sample, etc.)
            
        Returns:
            Tuple of (success: bool, s3_key: str)
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_filename = filename.replace(' ', '_')
        
        key = f"{self.app_prefix}/label-management/customer-requirements/{customer_id}/{timestamp}_{safe_filename}"
        
        return self.upload_file(file_content, key)
    
    def upload_label_asset(self, file_content: bytes, asset_type: str, filename: str) -> Tuple[bool, str]:
        """
        Upload reusable asset (logo, icon, etc)
        
        Args:
            file_content: File content as bytes
            asset_type: Type of asset (logos, icons, fonts)
            filename: Asset filename
            
        Returns:
            Tuple of (success: bool, s3_key: str)
        """
        safe_filename = filename.replace(' ', '_')
        key = f"{self.app_prefix}/label-management/assets/{asset_type}/{safe_filename}"
        
        return self.upload_file(file_content, key)
    
    def get_template_json(self, template_key: str) -> Optional[Dict]:
        """
        Get template JSON from S3
        
        Args:
            template_key: S3 key of the template
            
        Returns:
            Template dictionary or None if error
        """
        try:
            content = self.download_file(template_key)
            if content:
                return json.loads(content.decode('utf-8'))
            return None
            
        except Exception as e:
            logger.error(f"Error parsing template JSON: {e}")
            return None
    
    def save_template_json(self, template_data: Dict, customer_code: str, template_name: str) -> Tuple[bool, str]:
        """
        Save template JSON to S3
        
        Args:
            template_data: Template dictionary
            customer_code: Customer code
            template_name: Template name
            
        Returns:
            Tuple of (success: bool, s3_key: str)
        """
        safe_customer_code = customer_code.replace(' ', '_').lower()
        safe_template_name = template_name.replace(' ', '_').lower()
        
        key = f"{self.app_prefix}/label-management/templates/{safe_customer_code}/{safe_template_name}.json"
        
        try:
            json_content = json.dumps(template_data, indent=2)
            return self.upload_file(
                json_content.encode('utf-8'), 
                key, 
                content_type='application/json'
            )
        except Exception as e:
            return False, str(e)
    
    def list_templates(self, customer_code: str = None) -> List[Dict]:
        """
        List all templates or templates for specific customer
        
        Args:
            customer_code: Optional customer code to filter
            
        Returns:
            List of template files
        """
        if customer_code:
            safe_customer_code = customer_code.replace(' ', '_').lower()
            prefix = f"{self.app_prefix}/label-management/templates/{safe_customer_code}/"
        else:
            prefix = f"{self.app_prefix}/label-management/templates/"
        
        return self.list_files(prefix=prefix)
    
    def copy_file(self, source_key: str, dest_key: str) -> bool:
        """
        Copy file within S3
        
        Args:
            source_key: Source S3 key
            dest_key: Destination S3 key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': source_key
            }
            
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=dest_key
            )
            
            logger.info(f"Successfully copied {source_key} to {dest_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Error copying file: {e}")
            return False
    
    def batch_delete(self, keys: List[str]) -> Dict[str, List[str]]:
        """
        Delete multiple files at once
        
        Args:
            keys: List of S3 keys to delete
            
        Returns:
            Dictionary with 'deleted' and 'errors' lists
        """
        result = {'deleted': [], 'errors': []}
        
        if not keys:
            return result
        
        try:
            # S3 batch delete accepts max 1000 keys at once
            for i in range(0, len(keys), 1000):
                batch = keys[i:i+1000]
                
                response = self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={
                        'Objects': [{'Key': key} for key in batch]
                    }
                )
                
                if 'Deleted' in response:
                    result['deleted'].extend([obj['Key'] for obj in response['Deleted']])
                
                if 'Errors' in response:
                    result['errors'].extend([
                        f"{err['Key']}: {err['Message']}" 
                        for err in response['Errors']
                    ])
            
            logger.info(f"Batch delete complete. Deleted: {len(result['deleted'])}, Errors: {len(result['errors'])}")
            
        except ClientError as e:
            logger.error(f"Error in batch delete: {e}")
            result['errors'].append(str(e))
        
        return result
    
    def get_folder_size(self, prefix: str) -> Dict[str, float]:
        """
        Calculate total size of all files in a folder
        
        Args:
            prefix: Folder prefix
            
        Returns:
            Dictionary with size information
        """
        total_size = 0
        file_count = 0
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Skip .keep files when calculating size
                        if not obj['Key'].endswith('.keep'):
                            total_size += obj['Size']
                            file_count += 1
            
            return {
                'total_bytes': total_size,
                'total_mb': round(total_size / 1024 / 1024, 2),
                'total_gb': round(total_size / 1024 / 1024 / 1024, 2),
                'file_count': file_count
            }
            
        except ClientError as e:
            logger.error(f"Error calculating folder size: {e}")
            return {
                'total_bytes': 0,
                'total_mb': 0,
                'total_gb': 0,
                'file_count': 0
            }
    
    def create_folder(self, folder_path: str) -> bool:
        """
        Create a folder (prefix) in S3
        
        Args:
            folder_path: Folder path to create
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure folder path ends with /
            if not folder_path.endswith('/'):
                folder_path += '/'
            
            # Create a .keep file to make folder visible
            keep_file_key = f"{folder_path}.keep"
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=keep_file_key,
                Body=b'# This file keeps the folder structure',
                ContentType='text/plain'
            )
            
            logger.info(f"Created folder: {folder_path}")
            return True
            
        except ClientError as e:
            logger.error(f"Error creating folder: {e}")
            return False    
    # ==================== Invoice Management Specific Methods ====================
    
    def upload_invoice_file(self, file_content: bytes, filename: str, 
                           invoice_number: str = None) -> Tuple[bool, str]:
        """
        Upload single invoice attachment file
        
        Args:
            file_content: File content as bytes
            filename: Original filename (should be sanitized)
            invoice_number: Optional invoice number for reference
            
        Returns:
            Tuple of (success: bool, s3_key: str or error_message: str)
        """
        try:
            # Generate timestamp for uniqueness
            timestamp = int(datetime.now().timestamp() * 1000)
            
            # Clean filename (should already be sanitized, but double-check)
            safe_filename = filename.replace(' ', '_')
            
            # Generate S3 key
            s3_key = f"purchase-invoice-file/{timestamp}_{safe_filename}"
            
            # Determine content type
            content_type = self._get_content_type_from_filename(filename)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=file_content,
                ContentType=content_type
            )
            
            logger.info(f"Successfully uploaded invoice file: {s3_key}")
            return True, s3_key
            
        except ClientError as e:
            error_msg = f"Failed to upload invoice file {filename}: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error uploading {filename}: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def batch_upload_invoice_files(self, files: List[Tuple[bytes, str]]) -> Dict[str, Any]:
        """
        Batch upload multiple invoice files
        
        Args:
            files: List of tuples (file_content: bytes, filename: str)
            
        Returns:
            Dictionary with upload results:
            {
                'success': bool,
                'uploaded': List[str],  # S3 keys of successful uploads
                'failed': List[Dict],   # Failed uploads with error info
                'total': int,
                'success_count': int,
                'error_count': int
            }
        """
        results = {
            'success': False,
            'uploaded': [],
            'failed': [],
            'total': len(files),
            'success_count': 0,
            'error_count': 0
        }
        
        if not files:
            results['success'] = True
            return results
        
        logger.info(f"Starting batch upload of {len(files)} invoice files")
        
        for idx, (file_content, filename) in enumerate(files, 1):
            try:
                success, result = self.upload_invoice_file(file_content, filename)
                
                if success:
                    results['uploaded'].append(result)  # result is s3_key
                    results['success_count'] += 1
                    logger.info(f"[{idx}/{len(files)}] Uploaded: {filename}")
                else:
                    results['failed'].append({
                        'filename': filename,
                        'error': result  # result is error message
                    })
                    results['error_count'] += 1
                    logger.error(f"[{idx}/{len(files)}] Failed: {filename} - {result}")
                    
            except Exception as e:
                error_msg = f"Unexpected error with {filename}: {str(e)}"
                results['failed'].append({
                    'filename': filename,
                    'error': error_msg
                })
                results['error_count'] += 1
                logger.error(f"[{idx}/{len(files)}] Error: {error_msg}")
        
        # Set overall success if all files uploaded
        results['success'] = (results['error_count'] == 0)
        
        logger.info(f"Batch upload complete: {results['success_count']} succeeded, {results['error_count']} failed")
        
        return results
    
    def delete_invoice_file(self, s3_key: str) -> bool:
        """
        Delete invoice file from S3
        
        Args:
            s3_key: Full S3 key of the file to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Verify it's an invoice file
            if not s3_key.startswith('purchase-invoice-file/'):
                logger.warning(f"Attempted to delete non-invoice file: {s3_key}")
                return False
            
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            logger.info(f"Deleted invoice file: {s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting invoice file {s3_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting {s3_key}: {e}")
            return False
    
    def batch_delete_invoice_files(self, s3_keys: List[str]) -> Dict[str, List[str]]:
        """
        Delete multiple invoice files at once
        
        Args:
            s3_keys: List of S3 keys to delete
            
        Returns:
            Dictionary with 'deleted' and 'errors' lists
        """
        result = {'deleted': [], 'errors': []}
        
        if not s3_keys:
            return result
        
        # Filter to only invoice files
        invoice_keys = [k for k in s3_keys if k.startswith('purchase-invoice-file/')]
        
        if len(invoice_keys) != len(s3_keys):
            logger.warning(f"Filtered out {len(s3_keys) - len(invoice_keys)} non-invoice files")
        
        try:
            # S3 batch delete accepts max 1000 keys at once
            for i in range(0, len(invoice_keys), 1000):
                batch = invoice_keys[i:i+1000]
                
                response = self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={
                        'Objects': [{'Key': key} for key in batch]
                    }
                )
                
                if 'Deleted' in response:
                    result['deleted'].extend([obj['Key'] for obj in response['Deleted']])
                
                if 'Errors' in response:
                    result['errors'].extend([
                        f"{err['Key']}: {err['Message']}" 
                        for err in response['Errors']
                    ])
            
            logger.info(f"Batch delete complete. Deleted: {len(result['deleted'])}, Errors: {len(result['errors'])}")
            
        except ClientError as e:
            logger.error(f"Error in batch delete: {e}")
            result['errors'].append(str(e))
        
        return result
    
    def list_invoice_files(self, max_keys: int = 1000) -> List[Dict]:
        """
        List all invoice files in the purchase-invoice-file folder
        
        Args:
            max_keys: Maximum number of files to return
            
        Returns:
            List of file dictionaries with metadata
        """
        prefix = 'purchase-invoice-file/'
        return self.list_files(prefix=prefix, max_keys=max_keys)
    
    def get_invoice_file_url(self, s3_key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate presigned URL for invoice file download
        
        Args:
            s3_key: S3 key of the invoice file
            expiration: URL expiration time in seconds (default 1 hour)
            
        Returns:
            Presigned URL or None if error
        """
        # Verify it's an invoice file
        if not s3_key.startswith('purchase-invoice-file/'):
            logger.warning(f"Attempted to get URL for non-invoice file: {s3_key}")
            return None
        
        return self.get_presigned_url(s3_key, expiration)
    
    def _get_content_type_from_filename(self, filename: str) -> str:
        """
        Helper method to determine content type from filename
        
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