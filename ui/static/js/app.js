document.addEventListener('DOMContentLoaded', function() {
    console.log("DOM loaded, initializing...");

    // Set up UI controls
    setupUIControls();

    // Set up form submission
    setupFormSubmission();

    // Set up file uploads for both source and destination
    setupFileUpload('source');
    setupFileUpload('dest');
});

function setupUIControls() {
    // Range sliders
    setupRangeSliders();

    // New validation button
    const newValidationBtn = document.getElementById('new-validation-btn');
    if (newValidationBtn) {
        newValidationBtn.addEventListener('click', function() {
            document.getElementById('results').style.display = 'none';
            document.getElementById('validation-form-container').style.display = 'block';
            document.getElementById('validation-form').reset();

            // Clear file displays
            document.getElementById('source-selected-files').textContent = '';
            document.getElementById('dest-selected-files').textContent = '';

            // Reset file storage
            if (window.sourceFiles) window.sourceFiles.clear();
            if (window.destFiles) window.destFiles.clear();
        });
    }

    // View report button
    const viewReportBtn = document.getElementById('view-report-btn');
    if (viewReportBtn) {
        viewReportBtn.addEventListener('click', function() {
            const reportId = this.getAttribute('data-report-id');
            if (reportId) {
                window.open(`/reports/${reportId}`, '_blank');
            }
        });
    }
}

// Global file storage
window.sourceFiles = new Map();
window.destFiles = new Map();

function setupFileUpload(type) {
    const fileInput = document.getElementById(`${type}-file-upload`);
    const uploadBtn = document.getElementById(`${type}-upload-btn`);
    const selectedFilesDiv = document.getElementById(`${type}-selected-files`);
    const dropArea = document.getElementById(`${type}-drop-area`);

    if (!fileInput || !uploadBtn || !selectedFilesDiv || !dropArea) {
        console.error(`Missing elements for ${type} file upload`);
        return;
    }

    // Click to browse - prevent propagation to avoid double triggers
    uploadBtn.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        fileInput.click();
    });

    // Also make the drop area clickable
    dropArea.addEventListener('click', function(e) {
        if (e.target === uploadBtn) return; // Don't double-trigger if button was clicked
        fileInput.click();
    });

    // Remove any file type restrictions
    fileInput.removeAttribute('accept');

    // Handle file selection from input
    fileInput.addEventListener('change', function(e) {
        console.log(`File input change event for ${type}:`, e.target.files);
        if (e.target.files.length > 0) {
            addFiles(Array.from(e.target.files), type);
        }
    });

    // Prevent default behavior for drag events
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, function(e) {
            e.preventDefault();
            e.stopPropagation();
        });
    });

    // Visual feedback
    ['dragenter', 'dragover'].forEach(eventName => {
        dropArea.addEventListener(eventName, function() {
            dropArea.classList.add('highlight');
        });
    });

    ['dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, function() {
            dropArea.classList.remove('highlight');
        });
    });

    // Handle dropped files
    dropArea.addEventListener('drop', function(e) {
        console.log(`Drop event for ${type}:`, e.dataTransfer.files);
        const files = Array.from(e.dataTransfer.files);
        if (files.length > 0) {
            addFiles(files, type);
        }
    });
}

function addFiles(newFiles, type) {
    console.log(`Adding files for ${type}:`, newFiles);

    // Get the appropriate file storage
    const fileStorage = type === 'source' ? window.sourceFiles : window.destFiles;

    // Add new files to storage
    newFiles.forEach(file => {
        fileStorage.set(file.name, file);
    });

    // Update UI
    updateFileDisplay(type);

    // Update the hidden file input
    updateFileInput(type);
}

function updateFileDisplay(type) {
    const selectedFilesDiv = document.getElementById(`${type}-selected-files`);
    const fileStorage = type === 'source' ? window.sourceFiles : window.destFiles;

    if (fileStorage.size === 0) {
        selectedFilesDiv.textContent = '';
        return;
    }

    // Create file list display
    const fileList = Array.from(fileStorage.values()).map(f => {
        const ext = f.name.split('.').pop().toUpperCase();
        return `${f.name} (${ext})`;
    });

    selectedFilesDiv.textContent = `Selected: ${fileList.join(', ')}`;
}

function updateFileInput(type) {
    const fileInput = document.getElementById(`${type}-file-upload`);
    const fileStorage = type === 'source' ? window.sourceFiles : window.destFiles;

    // Create a new FileList from the stored files
    const dataTransfer = new DataTransfer();
    fileStorage.forEach(file => {
        dataTransfer.items.add(file);
    });

    // Update the input's files
    fileInput.files = dataTransfer.files;
    console.log(`Updated ${type} file input:`, fileInput.files);
}

function setupRangeSliders() {
    // Batch size slider (1-500)
    const batchSizeSlider = document.getElementById('batch-size-slider');
    const batchSizeInput = document.getElementById('batch-size');

    if (batchSizeSlider && batchSizeInput) {
        batchSizeSlider.addEventListener('input', function() {
            batchSizeInput.value = this.value;
        });

        batchSizeInput.addEventListener('input', function() {
            let value = parseInt(this.value);

            if (isNaN(value) || value < 1) {
                value = 1;
            } else if (value > 500) {
                value = 500;
            }

            this.value = value;
            batchSizeSlider.value = value;
        });
    }

    // Chunk size is now fixed at 1000
    const chunkSizeInput = document.getElementById('chunk-size');
    if (chunkSizeInput) {
        chunkSizeInput.value = 1000;
        chunkSizeInput.readOnly = true;
    }
}

function setupFormSubmission() {
    const validationForm = document.getElementById('validation-form');

    validationForm.addEventListener('submit', function(e) {
        e.preventDefault();

        // Check if files are selected
        if (window.sourceFiles.size === 0) {
            alert('Please upload at least one source file');
            return;
        }

        if (window.destFiles.size === 0) {
            alert('Please upload at least one destination file');
            return;
        }

        // Create FormData for file upload
        const formData = new FormData();

        // Add source files
        window.sourceFiles.forEach(file => {
            formData.append('source_files', file);
        });

        // Add destination files
        window.destFiles.forEach(file => {
            formData.append('dest_files', file);
        });

        // Add other form data
        formData.append('batch_size', document.getElementById('batch-size').value);
        formData.append('chunk_size', '1000'); // Fixed at 1000

        // Debug log
        console.log('Submitting form with files:');
        console.log('Source files:', Array.from(window.sourceFiles.values()).map(f => f.name));
        console.log('Dest files:', Array.from(window.destFiles.values()).map(f => f.name));

        // Show progress
        showProgress();

        // Submit validation request
        fetch('/api/run-validation', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            console.log('Validation start response:', data);
            if (data.success) {
                pollValidationStatus(data.run_id);
            } else {
                hideProgress();
                alert('Error starting validation: ' + (data.error || 'Unknown error'));
            }
        })
        .catch(error => {
            console.error('Error running validation:', error);
            hideProgress();
            alert('Error running validation. Please try again.');
        });
    });
}

// Rest of the functions remain the same...
function showProgress() {
    const progressElement = document.getElementById('validation-progress');
    progressElement.style.display = 'flex';
    updateProgressBar(0);
}

function hideProgress() {
    const progressElement = document.getElementById('validation-progress');
    progressElement.style.display = 'none';
}

function updateProgressBar(percentage) {
    const progressFill = document.getElementById('progress-fill');
    const progressText = document.getElementById('progress-text');

    progressFill.style.width = percentage + '%';
    progressText.textContent = percentage + '%';
}

function updateCurrentTable(tableName) {
    const currentTable = document.getElementById('current-table');
    if (currentTable) {
        currentTable.textContent = tableName ? `${tableName}` : '';
    }
}

function pollValidationStatus(runId) {
    let pollCount = 0;
    const maxPolls = 300; // 10 minutes maximum (300 * 2 seconds)

    const pollInterval = setInterval(function() {
        pollCount++;

        if (pollCount > maxPolls) {
            clearInterval(pollInterval);
            hideProgress();
            alert('Validation timeout: The process is taking too long. Please try again with smaller batch size.');
            return;
        }

        fetch('/api/validation-status/' + runId)
            .then(response => response.json())
            .then(data => {
                console.log('Status:', data); // Debug logging

                if (data.status === 'completed' || data.status === 'failed') {
                    clearInterval(pollInterval);
                    hideProgress();

                    if (data.status === 'completed') {
                        displayResults(data);
                    } else {
                        const errorMessage = data.error || 'Unknown error';
                        console.error('Validation failed:', errorMessage);
                        alert('Validation failed: ' + errorMessage);
                    }
                } else if (data.status === 'running') {
                    // Update progress
                    if (data.progress !== undefined) {
                        updateProgressBar(data.progress);
                    }
                    if (data.current_table) {
                        updateCurrentTable(data.current_table);
                    }
                } else {
                    // Handle unexpected status
                    console.warn('Unexpected status:', data.status);
                }
            })
            .catch(error => {
                console.error('Error checking validation status:', error);
                // Don't stop polling on network errors
            });
    }, 2000); // Poll every 2 seconds
}

function displayResults(data) {
    const validationForm = document.getElementById('validation-form-container');
    const resultsContainer = document.getElementById('results');
    const resultsTableBody = document.querySelector('#results-table tbody');

    // Clear existing results
    resultsTableBody.innerHTML = '';

    // Get results array
    const results = data.results || [];

    // If we have report_data, show summary
    if (data.report_data) {
        const summary = data.report_data.summary || {};
        const meta = data.report_data.meta || {};

        console.log('Report Summary:', summary);
        console.log('Report Meta:', meta);
    }

    // Set the report ID on the view report button
    if (data.report_id) {
        const viewReportBtn = document.getElementById('view-report-btn');
        if (viewReportBtn) {
            viewReportBtn.setAttribute('data-report-id', data.report_id);
            viewReportBtn.style.display = 'inline-block';
        }
    }

    // Populate results table
    results.forEach(result => {
        const row = document.createElement('tr');
        const matchStatus = result.match ? 'Match' : 'No Match';
        const matchClass = result.match ? 'match' : 'no-match';

        // Show more detailed information
        let detailsText = matchStatus;
        if (!result.match) {
            const details = [];
            if (result.different_rows > 0) details.push(`${result.different_rows} different`);
            if (result.missing_rows > 0) details.push(`${result.missing_rows} missing`);
            detailsText = details.length > 0 ? details.join(', ') : matchStatus;
        }

        row.innerHTML = `
            <td>${result.table_name}</td>
            <td>${result.source_count}</td>
            <td>${result.destination_count}</td>
            <td class="${matchClass}">${detailsText}</td>
            <td>${result.processing_time}s</td>
        `;
        resultsTableBody.appendChild(row);
    });

    // Show results, hide form
    validationForm.style.display = 'none';
    resultsContainer.style.display = 'block';
}

function showError(message) {
    console.error(message);
    alert(message);
}