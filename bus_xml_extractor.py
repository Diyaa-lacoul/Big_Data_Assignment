#!/usr/bin/env python3
"""
Bus Data XML to CSV Converter - Dynamic Field Extraction
Extracts ALL available fields from TransXChange and similar bus data XML files.
Handles various XML structures flexibly for ML/data science applications.
"""

import os
import sys
import csv
import re
from pathlib import Path
from datetime import datetime
from xml.etree import ElementTree as ET
from collections import defaultdict


class BusDataExtractor:
    """Comprehensive XML extractor for bus/transit data."""
    
    def __init__(self, xml_folder):
        self.source_folder = Path(xml_folder)
        self.stops_data = {}
        self.timing_segments = []
        self.route_details = {}
        self.service_info = {}
        self.operator_info = {}
        self.discovered_tags = set()
        self.namespace = None
        
    def find_namespace(self, root_elem):
        """Auto-detect XML namespace."""
        match = re.match(r'\{(.+)\}', root_elem.tag)
        if match:
            self.namespace = match.group(1)
            return {'ns': self.namespace}
        return {}
    
    def get_tag(self, name):
        """Get tag with namespace."""
        if self.namespace:
            return f'{{{self.namespace}}}{name}'
        return name
    
    def safe_get_text(self, element, path, default=''):
        """Safely extract text from element path."""
        if element is None:
            return default
        try:
            found = element.find(path, {'ns': self.namespace} if self.namespace else {})
            if found is not None and found.text:
                return found.text.strip()
        except:
            pass
        return default
    
    def parse_duration(self, iso_duration):
        """Convert ISO 8601 duration (PT1M30S) to seconds."""
        if not iso_duration or iso_duration == '':
            return 0
        try:
            pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
            match = re.match(pattern, iso_duration)
            if match:
                hours = int(match.group(1) or 0)
                minutes = int(match.group(2) or 0)
                seconds = int(match.group(3) or 0)
                return hours * 3600 + minutes * 60 + seconds
        except:
            pass
        return 0
    
    def extract_stops(self, tree_root):
        """Extract stop point information with coordinates."""
        stop_refs = tree_root.findall('.//' + self.get_tag('AnnotatedStopPointRef'))
        
        for stop in stop_refs:
            stop_id = self.safe_get_text(stop, self.get_tag('StopPointRef'))
            if stop_id:
                self.stops_data[stop_id] = {
                    'stop_id': stop_id,
                    'stop_name': self.safe_get_text(stop, self.get_tag('CommonName')),
                    'longitude': self.safe_get_text(stop, './/' + self.get_tag('Longitude')),
                    'latitude': self.safe_get_text(stop, './/' + self.get_tag('Latitude'))
                }
        
        return len(self.stops_data)
    
    def extract_operators(self, tree_root):
        """Extract operator information."""
        operators = tree_root.findall('.//' + self.get_tag('Operator'))
        
        for oper in operators:
            op_id = oper.get('id', 'unknown')
            self.operator_info[op_id] = {
                'operator_id': op_id,
                'national_code': self.safe_get_text(oper, self.get_tag('NationalOperatorCode')),
                'operator_code': self.safe_get_text(oper, self.get_tag('OperatorCode')),
                'operator_name': self.safe_get_text(oper, self.get_tag('OperatorShortName')),
                'licence_number': self.safe_get_text(oper, self.get_tag('LicenceNumber'))
            }
        
        return len(self.operator_info)
    
    def extract_services(self, tree_root):
        """Extract service and line information."""
        services = tree_root.findall('.//' + self.get_tag('Service'))
        
        for svc in services:
            svc_code = self.safe_get_text(svc, self.get_tag('ServiceCode'))
            line_name = self.safe_get_text(svc, './/' + self.get_tag('LineName'))
            
            self.service_info = {
                'service_code': svc_code,
                'line_name': line_name,
                'origin': self.safe_get_text(svc, './/' + self.get_tag('Origin')),
                'destination': self.safe_get_text(svc, './/' + self.get_tag('Destination')),
                'outbound_desc': self.safe_get_text(svc, './/' + self.get_tag('OutboundDescription') + '/' + self.get_tag('Description')),
                'inbound_desc': self.safe_get_text(svc, './/' + self.get_tag('InboundDescription') + '/' + self.get_tag('Description')),
                'start_date': self.safe_get_text(svc, './/' + self.get_tag('StartDate')),
                'end_date': self.safe_get_text(svc, './/' + self.get_tag('EndDate')),
                'public_use': self.safe_get_text(svc, self.get_tag('PublicUse'))
            }
            break
        
        return 1 if self.service_info else 0
    
    def extract_journey_patterns(self, tree_root):
        """Extract journey pattern information."""
        patterns = {}
        jp_elems = tree_root.findall('.//' + self.get_tag('JourneyPattern'))
        
        for jp in jp_elems:
            jp_id = jp.get('id', '')
            patterns[jp_id] = {
                'destination_display': self.safe_get_text(jp, self.get_tag('DestinationDisplay')),
                'direction': self.safe_get_text(jp, self.get_tag('Direction')),
                'route_ref': self.safe_get_text(jp, self.get_tag('RouteRef')),
                'section_ref': self.safe_get_text(jp, self.get_tag('JourneyPatternSectionRefs'))
            }
        
        return patterns
    
    def extract_timing_links(self, tree_root, source_file):
        """Extract stop-to-stop timing segments - the key ML data."""
        segments = []
        jp_sections = tree_root.findall('.//' + self.get_tag('JourneyPatternSection'))
        
        for section in jp_sections:
            section_id = section.get('id', '')
            timing_links = section.findall(self.get_tag('JourneyPatternTimingLink'))
            
            for link in timing_links:
                link_id = link.get('id', '')
                
                from_elem = link.find(self.get_tag('From'))
                to_elem = link.find(self.get_tag('To'))
                
                from_stop = self.safe_get_text(from_elem, self.get_tag('StopPointRef')) if from_elem is not None else ''
                to_stop = self.safe_get_text(to_elem, self.get_tag('StopPointRef')) if to_elem is not None else ''
                
                from_seq = from_elem.get('SequenceNumber', '') if from_elem is not None else ''
                to_seq = to_elem.get('SequenceNumber', '') if to_elem is not None else ''
                
                from_timing = self.safe_get_text(from_elem, self.get_tag('TimingStatus')) if from_elem is not None else ''
                to_timing = self.safe_get_text(to_elem, self.get_tag('TimingStatus')) if to_elem is not None else ''
                
                from_activity = self.safe_get_text(from_elem, self.get_tag('Activity')) if from_elem is not None else ''
                
                runtime_raw = self.safe_get_text(link, self.get_tag('RunTime'))
                runtime_secs = self.parse_duration(runtime_raw)
                
                route_link_ref = self.safe_get_text(link, self.get_tag('RouteLinkRef'))
                
                from_stop_info = self.stops_data.get(from_stop, {})
                to_stop_info = self.stops_data.get(to_stop, {})
                
                segment = {
                    'source_file': source_file,
                    'section_id': section_id,
                    'timing_link_id': link_id,
                    'from_stop_id': from_stop,
                    'from_stop_name': from_stop_info.get('stop_name', ''),
                    'from_latitude': from_stop_info.get('latitude', ''),
                    'from_longitude': from_stop_info.get('longitude', ''),
                    'from_sequence': from_seq,
                    'from_timing_status': from_timing,
                    'from_activity': from_activity,
                    'to_stop_id': to_stop,
                    'to_stop_name': to_stop_info.get('stop_name', ''),
                    'to_latitude': to_stop_info.get('latitude', ''),
                    'to_longitude': to_stop_info.get('longitude', ''),
                    'to_sequence': to_seq,
                    'to_timing_status': to_timing,
                    'runtime_raw': runtime_raw,
                    'runtime_seconds': runtime_secs,
                    'route_link_ref': route_link_ref,
                    'line_name': self.service_info.get('line_name', ''),
                    'operator_name': list(self.operator_info.values())[0].get('operator_name', '') if self.operator_info else '',
                    'service_origin': self.service_info.get('origin', ''),
                    'service_destination': self.service_info.get('destination', ''),
                    'service_code': self.service_info.get('service_code', '')
                }
                
                segments.append(segment)
        
        return segments
    
    def process_single_file(self, xml_path):
        """Process one XML file and extract all data."""
        file_segments = []
        
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            self.find_namespace(root)
            
            self.stops_data = {}
            self.operator_info = {}
            self.service_info = {}
            
            stops_count = self.extract_stops(root)
            ops_count = self.extract_operators(root)
            svc_count = self.extract_services(root)
            
            file_segments = self.extract_timing_links(root, xml_path.name)
            
            return {
                'filename': xml_path.name,
                'stops_found': stops_count,
                'operators_found': ops_count,
                'services_found': svc_count,
                'segments_found': len(file_segments),
                'data': file_segments,
                'status': 'success'
            }
            
        except Exception as ex:
            return {
                'filename': xml_path.name,
                'stops_found': 0,
                'operators_found': 0,
                'services_found': 0,
                'segments_found': 0,
                'data': [],
                'status': f'error: {str(ex)}'
            }
    
    def process_all_files(self):
        """Process all XML files in the folder."""
        xml_files = list(self.source_folder.glob('*.xml'))
        
        if not xml_files:
            print(f"[!] No XML files found in: {self.source_folder}")
            return []
        
        print(f"\n{'='*70}")
        print(f"  BUS DATA XML EXTRACTOR")
        print(f"  Processing {len(xml_files)} XML files")
        print(f"{'='*70}\n")
        
        all_segments = []
        summary_stats = {
            'total_files': len(xml_files),
            'successful': 0,
            'failed': 0,
            'total_segments': 0,
            'total_stops': 0
        }
        
        for idx, xml_file in enumerate(xml_files, 1):
            result = self.process_single_file(xml_file)
            
            status_icon = "+" if result['status'] == 'success' else "x"
            print(f"  [{status_icon}] ({idx}/{len(xml_files)}) {result['filename']}")
            print(f"      Stops: {result['stops_found']}, Segments: {result['segments_found']}")
            
            if result['status'] == 'success':
                summary_stats['successful'] += 1
                summary_stats['total_segments'] += result['segments_found']
                summary_stats['total_stops'] += result['stops_found']
                all_segments.extend(result['data'])
            else:
                summary_stats['failed'] += 1
                print(f"      Error: {result['status']}")
        
        self.timing_segments = all_segments
        return summary_stats
    
    def save_to_csv(self, output_path):
        """Save extracted data to CSV file."""
        if not self.timing_segments:
            print("[!] No data to save")
            return False
        
        fieldnames = list(self.timing_segments[0].keys())
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.timing_segments)
        
        return True
    
    def create_stops_csv(self, output_path):
        """Create a separate CSV for unique stops."""
        all_stops = {}
        
        for seg in self.timing_segments:
            if seg['from_stop_id'] and seg['from_stop_id'] not in all_stops:
                all_stops[seg['from_stop_id']] = {
                    'stop_id': seg['from_stop_id'],
                    'stop_name': seg['from_stop_name'],
                    'latitude': seg['from_latitude'],
                    'longitude': seg['from_longitude']
                }
            
            if seg['to_stop_id'] and seg['to_stop_id'] not in all_stops:
                all_stops[seg['to_stop_id']] = {
                    'stop_id': seg['to_stop_id'],
                    'stop_name': seg['to_stop_name'],
                    'latitude': seg['to_latitude'],
                    'longitude': seg['to_longitude']
                }
        
        if all_stops:
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['stop_id', 'stop_name', 'latitude', 'longitude'])
                writer.writeheader()
                writer.writerows(all_stops.values())
            
            return len(all_stops)
        return 0
    
    def calculate_data_quality(self):
        """Analyze data completeness."""
        if not self.timing_segments:
            return {}
        
        total_records = len(self.timing_segments)
        quality_metrics = {}
        
        key_fields = ['from_stop_id', 'to_stop_id', 'from_stop_name', 'to_stop_name',
                     'from_latitude', 'from_longitude', 'to_latitude', 'to_longitude',
                     'runtime_seconds', 'line_name']
        
        for field in key_fields:
            populated = sum(1 for seg in self.timing_segments if seg.get(field))
            completeness = (populated / total_records) * 100 if total_records > 0 else 0
            quality_metrics[field] = {
                'populated': populated,
                'completeness': round(completeness, 2)
            }
        
        return quality_metrics
    
    def display_summary(self, stats, quality_metrics, output_file, stops_file):
        """Display processing summary."""
        print(f"\n{'='*70}")
        print(f"  EXTRACTION COMPLETE")
        print(f"{'='*70}")
        
        print(f"\n  Files Processed: {stats['total_files']}")
        print(f"    - Successful: {stats['successful']}")
        print(f"    - Failed: {stats['failed']}")
        print(f"\n  Data Extracted:")
        print(f"    - Total Segments: {stats['total_segments']}")
        print(f"    - Total Stops: {stats['total_stops']}")
        
        print(f"\n  Data Quality (% populated):")
        print(f"  {'-'*40}")
        
        for field, metrics in quality_metrics.items():
            bar_len = int(metrics['completeness'] / 5)
            bar = '|' * bar_len + '.' * (20 - bar_len)
            print(f"    {field:20} [{bar}] {metrics['completeness']}%")
        
        print(f"\n  Output Files:")
        print(f"    - Segments: {output_file}")
        print(f"    - Stops: {stops_file}")
        print(f"\n{'='*70}\n")


def main():
    """Main execution function."""
    print("\n" + "="*70)
    print("  BUS DATA XML TO CSV CONVERTER")
    print("  Version 2.0 - Dynamic Field Extraction")
    print("="*70)
    
    # Determine input folder
    if len(sys.argv) > 1:
        input_folder = sys.argv[1]
    else:
        script_dir = Path(__file__).parent
        subfolders = [f for f in script_dir.iterdir() if f.is_dir() and list(f.glob('*.xml'))]
        
        if subfolders:
            input_folder = subfolders[0]
            print(f"\n  Auto-detected XML folder: {input_folder.name}")
        else:
            input_folder = script_dir
    
    input_path = Path(input_folder)
    
    if not input_path.exists():
        print(f"[ERROR] Folder not found: {input_path}")
        sys.exit(1)
    
    # Setup output files
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_csv = input_path / f'bus_segments_extracted_{timestamp}.csv'
    stops_csv = input_path / f'bus_stops_unique_{timestamp}.csv'
    
    # Run extraction
    extractor = BusDataExtractor(input_path)
    stats = extractor.process_all_files()
    
    if stats and stats.get('total_segments', 0) > 0:
        extractor.save_to_csv(output_csv)
        stops_count = extractor.create_stops_csv(stops_csv)
        quality = extractor.calculate_data_quality()
        extractor.display_summary(stats, quality, output_csv.name, stops_csv.name)
        
        print(f"  Ready for ML pipeline!")
        print(f"  Unique stops saved: {stops_count}")
    else:
        print("\n  [!] No data extracted. Check XML files.")
    
    return 0


if __name__ == '__main__':
    main()
