"""
Universal XML to CSV Extractor for Bus Data
Extracts ALL available fields regardless of XML structure
Optimized version
"""

import xml.etree.ElementTree as ET
import pandas as pd
import os
import sys
from datetime import datetime
from collections import defaultdict
import re

class XMLExtractor:
    def __init__(self, input_folder):
        self.input_folder = input_folder
        self.all_records = []
        self.all_fields = set()
        self.field_counts = defaultdict(int)
        self.files_processed = 0
        
    def parse_duration(self, duration_str):
        """Convert ISO 8601 duration (PT1M30S) to seconds"""
        if not duration_str or not isinstance(duration_str, str):
            return None
        match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
        if match:
            hours = int(match.group(1) or 0)
            minutes = int(match.group(2) or 0)
            seconds = int(match.group(3) or 0)
            return hours * 3600 + minutes * 60 + seconds
        return None

    def get_text(self, element, default=""):
        """Safely get text from element"""
        if element is not None and element.text:
            return element.text.strip()
        return default

    def extract_stops(self, root):
        """Extract stop information"""
        stops = {}
        
        for stop in root.iter():
            tag = stop.tag.split('}')[-1]
            if tag in ['StopPoint', 'AnnotatedStopPointRef']:
                stop_id = ""
                stop_data = {}
                
                for child in stop:
                    child_tag = child.tag.split('}')[-1]
                    
                    if child_tag in ['StopPointRef', 'AtcoCode']:
                        stop_id = self.get_text(child)
                        stop_data['stop_id'] = stop_id
                    elif child_tag == 'CommonName':
                        stop_data['stop_name'] = self.get_text(child)
                    elif child_tag == 'LocalityName':
                        stop_data['locality'] = self.get_text(child)
                    elif child_tag == 'Location':
                        for loc_child in child:
                            loc_tag = loc_child.tag.split('}')[-1]
                            if loc_tag == 'Latitude':
                                stop_data['latitude'] = self.get_text(loc_child)
                            elif loc_tag == 'Longitude':
                                stop_data['longitude'] = self.get_text(loc_child)
                
                if stop_id:
                    stops[stop_id] = stop_data
        
        return stops

    def extract_route_sections(self, root, stops, filename):
        """Extract route sections with timing links"""
        records = []
        
        # Find all JourneyPatternSections
        for section in root.iter():
            section_tag = section.tag.split('}')[-1]
            
            if section_tag == 'JourneyPatternSection':
                section_id = section.get('id', '')
                
                # Find all timing links in this section
                for link in section:
                    link_tag = link.tag.split('}')[-1]
                    
                    if link_tag == 'JourneyPatternTimingLink':
                        record = {
                            'source_file': filename,
                            'section_id': section_id,
                            'timing_link_id': link.get('id', '')
                        }
                        
                        for child in link:
                            child_tag = child.tag.split('}')[-1]
                            
                            if child_tag == 'From':
                                self._extract_stop_point(child, record, 'from', stops)
                            elif child_tag == 'To':
                                self._extract_stop_point(child, record, 'to', stops)
                            elif child_tag == 'RunTime':
                                runtime_raw = self.get_text(child)
                                record['runtime_raw'] = runtime_raw
                                record['runtime_seconds'] = self.parse_duration(runtime_raw)
                            elif child_tag == 'RouteLinkRef':
                                record['route_link_ref'] = self.get_text(child)
                        
                        if record.get('from_stop_id') or record.get('to_stop_id'):
                            records.append(record)
        
        return records

    def _extract_stop_point(self, element, record, prefix, stops):
        """Extract stop point data from From/To element"""
        for child in element:
            child_tag = child.tag.split('}')[-1]
            
            if child_tag == 'StopPointRef':
                stop_id = self.get_text(child)
                record[f'{prefix}_stop_id'] = stop_id
                if stop_id in stops:
                    record[f'{prefix}_stop_name'] = stops[stop_id].get('stop_name', '')
                    record[f'{prefix}_latitude'] = stops[stop_id].get('latitude', '')
                    record[f'{prefix}_longitude'] = stops[stop_id].get('longitude', '')
            elif child_tag == 'SequenceNumber':
                record[f'{prefix}_sequence'] = self.get_text(child)
            elif child_tag == 'TimingStatus':
                record[f'{prefix}_timing_status'] = self.get_text(child)
            elif child_tag == 'Activity':
                record[f'{prefix}_activity'] = self.get_text(child)

    def extract_services(self, root):
        """Extract service/route information"""
        service_data = {}
        
        for elem in root.iter():
            tag = elem.tag.split('}')[-1]
            
            if tag == 'LineName':
                service_data['line_name'] = self.get_text(elem)
            elif tag == 'ServiceCode':
                service_data['service_code'] = self.get_text(elem)
            elif tag == 'Origin' and 'service_origin' not in service_data:
                service_data['service_origin'] = self.get_text(elem)
            elif tag == 'Destination' and 'service_destination' not in service_data:
                service_data['service_destination'] = self.get_text(elem)
        
        return service_data

    def extract_operators(self, root):
        """Extract operator information"""
        for elem in root.iter():
            tag = elem.tag.split('}')[-1]
            if tag in ['OperatorShortName', 'TradingName']:
                return {'operator_name': self.get_text(elem)}
        return {}

    def process_file(self, filepath):
        """Process a single XML file"""
        filename = os.path.basename(filepath)
        
        try:
            tree = ET.parse(filepath)
            root = tree.getroot()
            
            # Extract reference data
            stops = self.extract_stops(root)
            service_data = self.extract_services(root)
            operator_data = self.extract_operators(root)
            
            # Extract timing links/segments
            records = self.extract_route_sections(root, stops, filename)
            
            # Enrich records with service/operator data
            for record in records:
                record.update({
                    'line_name': service_data.get('line_name', ''),
                    'operator_name': operator_data.get('operator_name', ''),
                    'service_origin': service_data.get('service_origin', ''),
                    'service_destination': service_data.get('service_destination', ''),
                    'service_code': service_data.get('service_code', '')
                })
                
                for key in record.keys():
                    self.all_fields.add(key)
                    if record[key]:
                        self.field_counts[key] += 1
            
            self.all_records.extend(records)
            self.files_processed += 1
            
            return len(records)
            
        except ET.ParseError as e:
            print(f"    ERROR parsing {filename}: {e}")
            return 0
        except Exception as e:
            print(f"    ERROR in {filename}: {e}")
            return 0

    def run(self):
        """Process all XML files in folder"""
        print("=" * 60)
        print("XML TO CSV EXTRACTOR")
        print("=" * 60)
        print(f"\nInput folder: {self.input_folder}")
        
        # Find all XML files
        xml_files = []
        for f in os.listdir(self.input_folder):
            if f.lower().endswith('.xml'):
                xml_files.append(os.path.join(self.input_folder, f))
        
        if not xml_files:
            print("ERROR: No XML files found!")
            return None
        
        print(f"Found {len(xml_files)} XML files\n")
        print("Processing", end="", flush=True)
        
        # Process each file
        for i, filepath in enumerate(xml_files):
            self.process_file(filepath)
            if (i + 1) % 10 == 0:
                print(".", end="", flush=True)
        
        print(f" Done!\n")
        
        print(f"{'=' * 60}")
        print("EXTRACTION COMPLETE")
        print(f"{'=' * 60}")
        print(f"Files processed: {self.files_processed}")
        print(f"Total records extracted: {len(self.all_records)}")
        
        if not self.all_records:
            print("WARNING: No records extracted!")
            return None
        
        # Create DataFrame
        df = pd.DataFrame(self.all_records)
        
        # Reorder columns (priority fields first)
        priority_cols = [
            'source_file', 'line_name', 'operator_name',
            'from_stop_id', 'from_stop_name', 'from_latitude', 'from_longitude',
            'to_stop_id', 'to_stop_name', 'to_latitude', 'to_longitude',
            'runtime_raw', 'runtime_seconds', 'from_sequence', 'to_sequence',
            'from_timing_status', 'to_timing_status', 'from_activity',
            'service_origin', 'service_destination', 'service_code',
            'section_id', 'timing_link_id', 'route_link_ref'
        ]
        
        other_cols = [c for c in df.columns if c not in priority_cols]
        final_cols = [c for c in priority_cols if c in df.columns] + other_cols
        df = df[final_cols]
        
        # Print field summary
        print(f"\n{'=' * 60}")
        print("EXTRACTED FIELDS")
        print(f"{'=' * 60}")
        print(f"{'Field':<35} {'Records':<10} {'%':<10}")
        print("-" * 55)
        
        for col in final_cols:
            non_null = df[col].notna().sum()
            non_empty = (df[col] != '').sum() if df[col].dtype == 'object' else non_null
            pct = (non_empty / len(df)) * 100
            print(f"{col:<35} {non_empty:<10} {pct:>6.1f}%")
        
        # Save CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.input_folder, f"bus_segments_extracted_{timestamp}.csv")
        df.to_csv(output_file, index=False)
        print(f"\n{'=' * 60}")
        print(f"OUTPUT SAVED: {output_file}")
        print(f"{'=' * 60}")
        
        # Also save unique stops
        if 'from_stop_id' in df.columns:
            # From stops
            from_stops = df[['from_stop_id', 'from_stop_name', 'from_latitude', 'from_longitude']].copy()
            from_stops.columns = ['stop_id', 'stop_name', 'latitude', 'longitude']
            
            # To stops
            to_stops = df[['to_stop_id', 'to_stop_name', 'to_latitude', 'to_longitude']].copy()
            to_stops.columns = ['stop_id', 'stop_name', 'latitude', 'longitude']
            
            # Combine and deduplicate
            stops_df = pd.concat([from_stops, to_stops]).drop_duplicates(subset=['stop_id'])
            stops_df = stops_df[stops_df['stop_id'].notna() & (stops_df['stop_id'] != '')]
            
            stops_file = os.path.join(self.input_folder, f"bus_stops_unique_{timestamp}.csv")
            stops_df.to_csv(stops_file, index=False)
            print(f"STOPS SAVED: {stops_file} ({len(stops_df)} unique stops)")
        
        # Data quality summary
        print(f"\n{'=' * 60}")
        print("DATA QUALITY SUMMARY")
        print(f"{'=' * 60}")
        
        key_fields = ['from_stop_id', 'to_stop_id', 'runtime_seconds', 'from_latitude', 'to_latitude']
        for field in key_fields:
            if field in df.columns:
                non_null = df[field].notna().sum()
                if df[field].dtype == 'object':
                    non_null = (df[field] != '').sum()
                pct = (non_null / len(df)) * 100
                status = "+" if pct > 80 else "~" if pct > 50 else "-"
                print(f"{status} {field}: {pct:.1f}% complete")
        
        return df


def main():
    if len(sys.argv) < 2:
        input_folder = os.getcwd()
        for item in os.listdir(input_folder):
            item_path = os.path.join(input_folder, item)
            if os.path.isdir(item_path):
                xml_files = [f for f in os.listdir(item_path) if f.endswith('.xml')]
                if xml_files:
                    input_folder = item_path
                    break
    else:
        input_folder = sys.argv[1]
    
    if not os.path.isabs(input_folder):
        input_folder = os.path.join(os.getcwd(), input_folder)
    
    if not os.path.exists(input_folder):
        print(f"ERROR: Folder not found: {input_folder}")
        sys.exit(1)
    
    extractor = XMLExtractor(input_folder)
    df = extractor.run()
    
    if df is not None:
        print(f"\nDone! Extracted {len(df)} records with {len(df.columns)} fields.")


if __name__ == "__main__":
    main()
