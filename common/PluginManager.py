import logging
import importlib.util
from pathlib import Path
from typing import Dict, Optional
from common.BasePlugin import BasePlugin
from common.Constants import Directories

class PluginManager:
    
    def __init__(self, products_dir: Path):
        self.products_dir = products_dir
        self.products_dir.mkdir(exist_ok=True)
        
        self.plugins = self.discover_products()
        self._plugin_instances = {}
        
        self.product_paths = {}
        for folder_name in self.plugins.keys():
            product_dir = self.products_dir / folder_name
            self.product_paths[folder_name] = {
                Directories.INBOX: product_dir / Directories.INBOX,
                Directories.PROCESSING: product_dir / Directories.PROCESSING,
                Directories.PROCESSED: product_dir / Directories.PROCESSED,
                Directories.FAILED: product_dir / Directories.FAILED,
                Directories.LOGS: product_dir / Directories.LOGS
            }
            
            for path in self.product_paths[folder_name].values():
                path.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"Loaded {len(self.plugins)} product(s): {list(self.plugins.keys())}")
    
    def discover_products(self) -> Dict[str, type]:
        plugins = {}
        
        for product_dir in self.products_dir.iterdir():
            if not product_dir.is_dir():
                continue
            
            folder_name = product_dir.name
            plugin_files = list(product_dir.glob('*Plugin.py'))
            
            if not plugin_files:
                logging.warning(f"No plugin file found in {folder_name}/, skipping")
                continue
            
            if len(plugin_files) > 1:
                logging.warning(f"Multiple plugin files in {folder_name}/, using first: {plugin_files[0].name}")
            
            plugin_file = plugin_files[0]
            plugin_class_name = plugin_file.stem
            
            try:
                spec = importlib.util.spec_from_file_location(
                    f"products.{folder_name}.{plugin_class_name}",
                    plugin_file
                )
                
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    
                    if hasattr(module, plugin_class_name):
                        plugin_class = getattr(module, plugin_class_name)
                        plugins[folder_name] = plugin_class
                        logging.info(f"Loaded plugin: {folder_name} ({plugin_class_name})")
                    else:
                        logging.error(
                            f"Plugin file {plugin_file.name} does not contain class {plugin_class_name}"
                        )
                else:
                    logging.error(f"Could not load spec for {plugin_file}")
                    
            except Exception as e:
                logging.error(f"Failed to load plugin from {folder_name}: {str(e)}")
                continue
        
        if not plugins:
            raise RuntimeError(
                "No product plugins found! Ensure products/<product_name>/<Product>Plugin.py exists"
            )
        
        return plugins
    
    def get_plugin(self, folder_name: str) -> Optional[BasePlugin]:
        if folder_name not in self._plugin_instances:
            plugin_class = self.plugins.get(folder_name)
            if plugin_class:
                self._plugin_instances[folder_name] = plugin_class()
                logging.debug(f"Instantiated plugin for {folder_name}")
            else:
                return None
        return self._plugin_instances.get(folder_name)
    
    def get_product_paths(self, folder_name: str) -> Optional[Dict[str, Path]]:
        return self.product_paths.get(folder_name)
    
    def get_all_products(self) -> list:
        return list(self.plugins.keys())
