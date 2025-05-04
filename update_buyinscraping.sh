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
REPO_URL="https://github.com/joy95mh/BuyinScraping.git"
INSTALL_DIR="/home/itdev/BuyinScraping"
BACKUP_DIR="/home/itdev/BuyinScraping_backup_$(date +%Y%m%d_%H%M%S)"
TEMP_CLONE_DIR="/home/itdev/BuyinScraping_temp"
VENV_NAME="venv"

print_status "Starting update process for BuyinScraping project"

# Check if the project directory exists
if [ ! -d "$INSTALL_DIR" ]; then
  print_error "Error: Directory $INSTALL_DIR does not exist!"
  print_status "This script is for updating an existing installation."
  print_status "To install from scratch, clone the repository directly:"
  print_status "git clone $REPO_URL $INSTALL_DIR"
  exit 1
fi

# Make a list of important files/directories to preserve
print_status "Identifying important data to preserve..."

# Directories to preserve
PRESERVE_DIRS=(
  "input"                # Input Excel files
  "logs"                 # Log files
  "notebooks"            # Jupyter notebooks
  "output"               # Output files if any
)

# Files to preserve
PRESERVE_FILES=(
  "input_folder_config.json"  # Configuration
  "run_jupyter.sh"            # Jupyter script
)

# Create backup of the entire project
print_status "Creating backup of current installation at $BACKUP_DIR"
cp -R "$INSTALL_DIR" "$BACKUP_DIR"
print_success "Backup created successfully"

# Clone the repository to a temporary location
print_status "Cloning latest version from $REPO_URL to temporary directory"
if [ -d "$TEMP_CLONE_DIR" ]; then
  rm -rf "$TEMP_CLONE_DIR"
fi
git clone "$REPO_URL" "$TEMP_CLONE_DIR"

# Prepare for swap
print_status "Preparing to update installation..."

# Create temporary directory for preserved files
PRESERVED_DIR="/home/itdev/BuyinScraping_preserved"
if [ -d "$PRESERVED_DIR" ]; then
  rm -rf "$PRESERVED_DIR"
fi
mkdir -p "$PRESERVED_DIR"

# Copy important directories to preserve
for dir in "${PRESERVE_DIRS[@]}"; do
  if [ -d "$INSTALL_DIR/$dir" ]; then
    print_status "Preserving directory: $dir"
    mkdir -p "$PRESERVED_DIR/$dir"
    cp -R "$INSTALL_DIR/$dir" "$PRESERVED_DIR/$(dirname $dir)"
  fi
done

# Copy important files to preserve
for file in "${PRESERVE_FILES[@]}"; do
  if [ -f "$INSTALL_DIR/$file" ]; then
    print_status "Preserving file: $file"
    mkdir -p "$PRESERVED_DIR/$(dirname $file)"
    cp "$INSTALL_DIR/$file" "$PRESERVED_DIR/$file"
  fi
done

# Preserve virtual environment if it exists and user wants to keep it
if [ -d "$INSTALL_DIR/$VENV_NAME" ]; then
  read -p "Do you want to preserve the virtual environment? (y/n): " preserve_venv
  if [[ $preserve_venv == "y" || $preserve_venv == "Y" ]]; then
    print_status "Preserving virtual environment..."
    # Just note it's being preserved (we'll move it later)
    PRESERVE_VENV=true
  else
    print_status "Virtual environment will be recreated"
    PRESERVE_VENV=false
  fi
fi

# Swap old with new
print_status "Replacing old installation with new version..."

# Preserve the virtual environment if requested
if [ "$PRESERVE_VENV" = true ]; then
  print_status "Moving virtual environment out of the way..."
  mv "$INSTALL_DIR/$VENV_NAME" "/home/itdev/${VENV_NAME}_temp"
fi

# Remove old installation (except .git if it exists to maintain git history)
if [ -d "$INSTALL_DIR/.git" ]; then
  print_status "Preserving Git history..."
  GIT_DIR="$INSTALL_DIR/.git"
  find "$INSTALL_DIR" -mindepth 1 -not -path "$INSTALL_DIR/.git*" -delete
else
  rm -rf "$INSTALL_DIR"
  mkdir -p "$INSTALL_DIR"
fi

# Copy new installation
print_status "Copying new code from temporary clone..."
cp -R "$TEMP_CLONE_DIR/." "$INSTALL_DIR/"

# Restore preserved files and directories
print_status "Restoring preserved data..."
for dir in "${PRESERVE_DIRS[@]}"; do
  if [ -d "$PRESERVED_DIR/$dir" ]; then
    print_status "Restoring directory: $dir"
    if [ ! -d "$INSTALL_DIR/$dir" ]; then
      mkdir -p "$INSTALL_DIR/$dir"
    fi
    cp -R "$PRESERVED_DIR/$dir/." "$INSTALL_DIR/$dir/"
  fi
done

for file in "${PRESERVE_FILES[@]}"; do
  if [ -f "$PRESERVED_DIR/$file" ]; then
    print_status "Restoring file: $file"
    mkdir -p "$INSTALL_DIR/$(dirname $file)"
    cp "$PRESERVED_DIR/$file" "$INSTALL_DIR/$file"
  fi
done

# Restore virtual environment if preserved
if [ "$PRESERVE_VENV" = true ]; then
  print_status "Restoring virtual environment..."
  rm -rf "$INSTALL_DIR/$VENV_NAME" 2>/dev/null || true
  mv "/home/itdev/${VENV_NAME}_temp" "$INSTALL_DIR/$VENV_NAME"
else
  # Create a new virtual environment
  print_status "Creating new virtual environment..."
  cd "$INSTALL_DIR"
  python3 -m venv "$VENV_NAME"
  
  # Activate and install requirements
  print_status "Installing required packages..."
  source "$VENV_NAME/bin/activate"
  pip install --upgrade pip
  pip install scrapy selenium pandas openpyxl lxml urllib3 undetected-chromedriver fake-useragent
  
  # Additional packages needed for Jupyter if present
  if [ -d "$INSTALL_DIR/notebooks" ]; then
    pip install jupyter notebook ipywidgets matplotlib
  fi
  
  deactivate
fi

# Clean up
print_status "Cleaning up..."
rm -rf "$TEMP_CLONE_DIR"
rm -rf "$PRESERVED_DIR"

# Apply Linux compatibility to main.py
if [ -f "$INSTALL_DIR/main.py" ]; then
  print_status "Checking main.py for Linux compatibility..."
  
  # Check if input_folder_config.json exists, if not create it
  if [ ! -f "$INSTALL_DIR/input_folder_config.json" ]; then
    print_status "Creating Linux-compatible input_folder_config.json..."
    cat > "$INSTALL_DIR/input_folder_config.json" << EOF
{
  "input_folder": "/mnt/shared/BuyinScraping/input",
  "local_input_folder": "/home/itdev/BuyinScraping/input"
}
EOF
  fi
  
  # Create necessary directories
  mkdir -p "$INSTALL_DIR/logs/processes" "$INSTALL_DIR/input"
  
  print_status "Main script ready for Linux"
fi

print_success "Update complete!"
print_success "Your BuyinScraping installation has been updated while preserving your data."
print_success "A backup of the old installation is available at: $BACKUP_DIR"
print_status "To run the updated code: cd $INSTALL_DIR && source $VENV_NAME/bin/activate && python main.py"

if [ -f "$INSTALL_DIR/run_jupyter.sh" ]; then
  print_status "To run Jupyter Notebook: $INSTALL_DIR/run_jupyter.sh"
fi 