#!/bin/bash

# Exit on error
set -e

# Define colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Print colored messages
function print_status() {
  echo -e "${BLUE}>>> $1${NC}"
}

function print_success() {
  echo -e "${GREEN}>>> $1${NC}"
}

function print_error() {
  echo -e "${RED}>>> $1${NC}"
}

# Set variables
INSTALL_DIR="/home/itdev/BuyinScraping"
VENV_NAME="venv"
JUPYTER_PORT=9898
JUPYTER_CONFIG_DIR="${HOME}/.jupyter"

print_status "Setting up Jupyter Notebook for BuyinScraping project"

# Check if the project directory exists
if [ ! -d "$INSTALL_DIR" ]; then
  print_error "Error: Directory $INSTALL_DIR does not exist!"
  print_status "Please run the BuyinScraping setup script first."
  exit 1
fi

# Make sure we're in the project directory
cd "$INSTALL_DIR"

# Make sure the virtual environment exists
if [ ! -d "$VENV_NAME" ]; then
  print_status "Creating virtual environment..."
  python3 -m venv "$VENV_NAME"
fi

# Activate the virtual environment
print_status "Activating virtual environment..."
source "$VENV_NAME/bin/activate"

# Install Jupyter and required packages
print_status "Installing Jupyter and required packages..."
pip install jupyter notebook ipywidgets pandas matplotlib fake-useragent

# Create Jupyter config directory if it doesn't exist
if [ ! -d "$JUPYTER_CONFIG_DIR" ]; then
  mkdir -p "$JUPYTER_CONFIG_DIR"
fi

# Create custom Jupyter configuration
print_status "Creating Jupyter configuration file..."
cat > "$JUPYTER_CONFIG_DIR/jupyter_notebook_config.py" << EOF
# Configuration file for Jupyter Notebook

c = get_config()  #noqa

# Basic configuration
c.NotebookApp.ip = '0.0.0.0'  # Listen on all interfaces
c.NotebookApp.port = $JUPYTER_PORT
c.NotebookApp.open_browser = False
c.NotebookApp.allow_origin = '*'
c.NotebookApp.notebook_dir = '$INSTALL_DIR'

# Security - change to True for production
c.NotebookApp.allow_remote_access = True

# Allow connections from all origins (be careful in production)
c.NotebookApp.allow_origin_pat = '.*'

# Disable cross-site request forgery protection (use only for testing)
# c.NotebookApp.disable_check_xsrf = True

# Logging
c.NotebookApp.log_level = 'INFO'

# Enable all available kernels
c.MultiKernelManager.default_kernel_name = 'python3'

# Password authentication
# To generate a password: from notebook.auth import passwd; passwd()
# or use the script below
# c.NotebookApp.password = ''  # Will be added by the script
EOF

# Create a notebooks directory
print_status "Creating notebooks directory..."
mkdir -p "$INSTALL_DIR/notebooks"

# Create a sample notebook
print_status "Creating a sample notebook..."
cat > "$INSTALL_DIR/notebooks/BuyinScraping_Demo.ipynb" << EOF
{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# BuyinScraping Project Demo\n",
    "\n",
    "This notebook demonstrates how to use the BuyinScraping project."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "source": [
    "# Add project directory to path\n",
    "import sys\n",
    "import os\n",
    "\n",
    "# Make sure we can import from the project root\n",
    "project_root = os.path.abspath(os.path.join(os.getcwd(), '..'))\n",
    "if project_root not in sys.path:\n",
    "    sys.path.append(project_root)\n",
    "\n",
    "print(f\"Project root: {project_root}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "source": [
    "# Import project modules\n",
    "import json\n",
    "import glob\n",
    "import pandas as pd\n",
    "from IPython.display import display, HTML\n",
    "\n",
    "# Function to list available input files\n",
    "def list_input_files():\n",
    "    input_dir = os.path.join(project_root, 'input')\n",
    "    if not os.path.exists(input_dir):\n",
    "        os.makedirs(input_dir)\n",
    "    return [f for f in os.listdir(input_dir) if f.endswith(('.xlsx', '.xlsm'))]\n",
    "\n",
    "print(\"Available input files:\")\n",
    "files = list_input_files()\n",
    "if files:\n",
    "    for f in files:\n",
    "        print(f\"- {f}\")\n",
    "else:\n",
    "    print(\"No input files found. Please upload Excel files to the input directory.\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "source": [
    "# List the available spiders\n",
    "print(\"Available spiders in BuyinScraping:\")\n",
    "\n",
    "try:\n",
    "    from main import SPIDER_MAPPING\n",
    "    for name in sorted(SPIDER_MAPPING.keys()):\n",
    "        print(f\"- {name}\")\n",
    "except ImportError:\n",
    "    print(\"Could not import SPIDER_MAPPING from main.py\")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.10"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
EOF

# Ask if user wants to set a password
print_status "Would you like to set a password for Jupyter Notebook? (y/n)"
read -r set_password

if [[ $set_password == "y" || $set_password == "Y" ]]; then
  print_status "Setting up password authentication..."
  
  # Create a temporary script to generate the password hash
  cat > /tmp/jupyter_passwd.py << EOF
from notebook.auth import passwd
import json
import os

password = input("Enter password for Jupyter Notebook: ")
hashed = passwd(password)
print(f"Password hash: {hashed}")

# Write the hash to the config file
config_file = os.path.expanduser('~/.jupyter/jupyter_notebook_config.py')
with open(config_file, 'a') as f:
    f.write(f"\n# Password authentication\nc.NotebookApp.password = '{hashed}'  # Added by setup script\n")

print("Password configuration added to ~/.jupyter/jupyter_notebook_config.py")
EOF

  # Run the password script
  python /tmp/jupyter_passwd.py
  rm /tmp/jupyter_passwd.py
else
  print_status "Skipping password setup. Jupyter will be accessible without authentication!"
fi

# Create a systemd service file for running Jupyter as a service
print_status "Creating systemd service file (will require sudo to install)..."
cat > /tmp/jupyter-notebook.service << EOF
[Unit]
Description=Jupyter Notebook Server for BuyinScraping
After=network.target

[Service]
Type=simple
User=$(whoami)
ExecStart=${INSTALL_DIR}/${VENV_NAME}/bin/jupyter notebook --config=${JUPYTER_CONFIG_DIR}/jupyter_notebook_config.py
WorkingDirectory=${INSTALL_DIR}
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
EOF

print_status "To install the systemd service, run the following command (requires sudo):"
print_status "sudo cp /tmp/jupyter-notebook.service /etc/systemd/system/ && sudo systemctl daemon-reload"

# Create a script to run Jupyter manually
print_status "Creating a script to run Jupyter manually..."
cat > "${INSTALL_DIR}/run_jupyter.sh" << EOF
#!/bin/bash
# Script to start Jupyter Notebook for BuyinScraping

# Activate the virtual environment
source "${INSTALL_DIR}/${VENV_NAME}/bin/activate"

# Start Jupyter Notebook
jupyter notebook --config="${JUPYTER_CONFIG_DIR}/jupyter_notebook_config.py"
EOF

chmod +x "${INSTALL_DIR}/run_jupyter.sh"

# Check for firewall status
print_status "Checking firewall status..."
if command -v firewall-cmd &> /dev/null; then
  print_status "firewalld detected. To open the Jupyter port, run:"
  print_status "sudo firewall-cmd --permanent --add-port=${JUPYTER_PORT}/tcp && sudo firewall-cmd --reload"
elif command -v ufw &> /dev/null; then
  print_status "ufw detected. To open the Jupyter port, run:"
  print_status "sudo ufw allow ${JUPYTER_PORT}/tcp"
else
  print_status "No known firewall detected. Make sure port ${JUPYTER_PORT} is open."
fi

# Network check
print_status "Running network check for Jupyter port..."
server_ip=$(hostname -I | awk '{print $1}')
print_status "Server IP address: $server_ip"

# Check if we can bind to the port
print_status "Testing if we can bind to port ${JUPYTER_PORT}..."
python3 -c "import socket; s=socket.socket(); s.bind(('0.0.0.0', ${JUPYTER_PORT})); s.close(); print('Port binding test successful')" || print_error "Port ${JUPYTER_PORT} is already in use!"

# Final instructions
print_success "Jupyter Notebook setup is complete!"
print_success "To start Jupyter manually: ${INSTALL_DIR}/run_jupyter.sh"
print_success "To access Jupyter Notebook: http://${server_ip}:${JUPYTER_PORT}"
print_success "Make sure your firewall allows connections to port ${JUPYTER_PORT}"

# Deactivate the virtual environment
deactivate 