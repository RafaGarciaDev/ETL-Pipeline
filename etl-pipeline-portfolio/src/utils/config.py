"""
Configuration Management Module
Handles loading and accessing configuration from YAML files.
"""

import yaml
import os
from typing import Any, Dict, Optional
from pathlib import Path


class Config:
    """
    Configuration manager for ETL pipeline.
    
    Loads configuration from YAML files and provides
    easy access to config values.
    """
    
    def __init__(self, config_path: str = 'config/pipeline.yaml'):
        """
        Initialize configuration.
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """
        Load configuration from YAML file.
        
        Returns:
            Configuration dictionary
        """
        if not Path(self.config_path).exists():
            # Return default config if file doesn't exist
            return self._default_config()
        
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Replace environment variables
        config = self._replace_env_vars(config)
        
        return config
    
    def _replace_env_vars(self, config: Any) -> Any:
        """
        Replace ${ENV_VAR} placeholders with environment variables.
        
        Args:
            config: Configuration value (dict, list, or str)
            
        Returns:
            Configuration with env vars replaced
        """
        if isinstance(config, dict):
            return {k: self._replace_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._replace_env_vars(item) for item in config]
        elif isinstance(config, str) and config.startswith('${') and config.endswith('}'):
            env_var = config[2:-1]
            return os.getenv(env_var, config)
        else:
            return config
    
    def _default_config(self) -> Dict:
        """
        Get default configuration.
        
        Returns:
            Default configuration dictionary
        """
        return {
            'extract': {
                'csv': {
                    'files': []
                },
                'api': {
                    'base_url': '',
                    'api_key': '',
                    'endpoint': '',
                    'params': {}
                }
            },
            'transform': {
                'type_conversions': {}
            },
            'load': {
                'database': {
                    'connection_string': 'postgresql://user:password@localhost:5432/db',
                    'schema': 'public',
                    'table_name': 'etl_data',
                    'strategy': 'append'
                }
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g., 'extract.csv.files')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g., 'extract.csv.files')
            value: Value to set
        """
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def save(self, output_path: Optional[str] = None) -> None:
        """
        Save configuration to YAML file.
        
        Args:
            output_path: Path to save config (uses original path if None)
        """
        path = output_path or self.config_path
        
        with open(path, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)
