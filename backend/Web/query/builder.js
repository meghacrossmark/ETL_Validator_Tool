document.addEventListener('DOMContentLoaded', function() {
    const selectedTable = localStorage.getItem('selectedTable');
    const headers = localStorage.getItem('col_headers');
    if (selectedTable) {
        document.getElementById('table').value = selectedTable;
    }

    const selector = document.getElementById('column_selector');
    const col_condition = document.getElementById('column_condition');
    selector.innerHTML = '';
    col_condition.innerHTML = '';
    const defaultOption = document.createElement('option');
    defaultOption.value = '*';
    defaultOption.textContent = '*';
    defaultOption.selected = true;
    selector.appendChild(defaultOption);
    headersArr = headers.split(",").map(column => column.trim());
    headersArr.forEach(function(column) {
        if (column) {
            const option = document.createElement('option');
            option.value = column;
            option.textContent = column;
            selector.appendChild(option);
        }
    });

    headersArr.forEach(function(column) {
        if (column) {
            const option = document.createElement('option');
            option.value = column;
            option.textContent = column;
            col_condition.appendChild(option);
        }
    });
});

// Generate SQL query
function generateQuery() {
    const table = document.getElementById('table').value;
    const selector = document.getElementById('column_selector').value;
    const col_condition = document.getElementById('column_condition').value;
    const condition = document.getElementById('condition').value;

    let query = `SELECT ${selector} FROM ${table}`;

    if (condition.trim() !== "") {
        query += ` WHERE ${col_condition} ${condition}`;
    }

    // Display the generated SQL query
    document.getElementById('generated-sql').textContent = query;
}

document.getElementById("copyQuery").addEventListener("click", function() {
    const sqlQuery = document.getElementById("generated-sql").textContent;
    localStorage.setItem(localStorage.getItem('queryType')+"_query", sqlQuery);
});
