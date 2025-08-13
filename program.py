import psycopg2
import json
import xml.etree.ElementTree as ET
from tkinter import Tk, filedialog

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname="",     # Enter your database name
    user="",       # Enter your username
    password="",   # Enter your password
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Order of tables for export/import (FK dependencies are preserved)
TABLES = [
    "Countries", "Genres", "Languages", "Statuses",
    "Branches", "Employees", "Customers",
    "Movies", "Rentals", "Payments"
]

# Export to JSON
def export_to_json():
    root = Tk()
    root.withdraw()  # Hide the main Tk window
    filename = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON files", "*.json")])
    if not filename:
        print("Export cancelled.")
        return

    data = {}
    for table in TABLES:
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        data[table] = [dict(zip(colnames, row)) for row in rows]

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, default=str)
    print(f"Data exported to {filename}")

# Export to XML
def export_to_xml():
    root = Tk()
    root.withdraw()  # Hide the main Tk window
    filename = filedialog.asksaveasfilename(defaultextension=".xml", filetypes=[("XML files", "*.xml")])
    if not filename:
        print("Export cancelled.")
        return

    root_xml = ET.Element("Database")
    for table in TABLES:
        table_element = ET.SubElement(root_xml, table)
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        for row in rows:
            row_elem = ET.SubElement(table_element, "Row")
            for col, val in zip(colnames, row):
                col_elem = ET.SubElement(row_elem, col)
                col_elem.text = str(val)

    tree = ET.ElementTree(root_xml)
    tree.write(filename, encoding="utf-8", xml_declaration=True)
    print(f"Data exported to {filename}")

# Import from JSON
def import_from_json():
    root = Tk()
    root.withdraw()  # Hide the main Tk window
    filename = filedialog.askopenfilename(filetypes=[("JSON files", "*.json")])
    if not filename:
        print("Import cancelled.")
        return

    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    for table in TABLES:
        if table not in data:
            continue
        for row in data[table]:
            columns = ', '.join(row.keys())
            placeholders = ', '.join(['%s'] * len(row))
            values = list(row.values())
            try:
                # Insert row into the table, skip if conflict occurs
                cur.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING", values)
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                print(f"Error importing into {table} (id? {row.get('id', '?')}): {e.pgerror.strip()}")

    print(f"Data imported from {filename}")

# Import from XML without FK validation
def import_from_xml():
    root = Tk()
    root.withdraw()  # Hide the main Tk window
    filename = filedialog.askopenfilename(filetypes=[("XML files", "*.xml")])
    if not filename:
        print("Import cancelled.")
        return

    tree = ET.parse(filename)
    root_xml = tree.getroot()
    for table_element in root_xml:
        table = table_element.tag
        if table not in TABLES:
            continue
        for row_elem in table_element.findall("Row"):
            row_data = {child.tag: child.text for child in row_elem}
            columns = ', '.join(row_data.keys())
            placeholders = ', '.join(['%s'] * len(row_data))
            values = list(row_data.values())
            try:
                # Insert row into the table, skip if conflict occurs
                cur.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING", values)
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                print(f"Error importing into {table} (id? {row_data.get('id', '?')}): {e.pgerror.strip()}")
    print(f"Data imported from {filename}")

# Main menu
def main():
    print("1. Export to JSON")
    print("2. Export to XML")
    print("3. Import from JSON")
    print("4. Import from XML")
    choice = input("Choose an option (1-4): ")

    if choice == "1":
        export_to_json()
    elif choice == "2":
        export_to_xml()
    elif choice == "3":
        import_from_json()
    elif choice == "4":
        import_from_xml()
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()
