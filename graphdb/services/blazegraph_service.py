import requests
import xml.etree.ElementTree as ET

class BlazegraphService:
    def __init__(self, base_url=None, ip='localhost', port='9999'):
        if base_url:
            self.base_url = base_url
        else:
            self.set_connection(ip, port)

    def set_connection(self, ip, port):
        self.base_url = f'http://{ip}:{port}/blazegraph'
        return {'status': 'Connection details updated successfully'}  # Replace with your Blazegraph server URL

    def create_namespace(self, name):
        headers = {
            'Content-Type': 'text/plain',
        }

        data = f"""
        com.bigdata.rdf.store.AbstractTripleStore.textIndex=false
        com.bigdata.rdf.store.AbstractTripleStore.axiomsClass=com.bigdata.rdf.axioms.NoAxioms
        com.bigdata.rdf.sail.isolatableIndices=false
        com.bigdata.rdf.sail.truthMaintenance=false
        com.bigdata.namespace.{name}.spo.com.bigdata.btree.BTree.branchingFactor=1024
        com.bigdata.rdf.sail.namespace={name}
        com.bigdata.rdf.store.AbstractTripleStore.quads=false
        com.bigdata.rdf.store.AbstractTripleStore.geoSpatial=false
        com.bigdata.rdf.store.AbstractTripleStore.statementIdentifiers=false
        """

        response = requests.post(f'{self.base_url}/namespace', headers=headers, data=data)
        print("Create Namespace Response Status Code:", response.status_code)
        print("Create Namespace Response Text:", response.text)

        if response.status_code == 201:
            return {'status': 'Namespace created successfully'}
        else:
            response.raise_for_status()

    def upload_ttl(self, ttl_file, filename, graph_id):
        files = {'file': (filename, ttl_file)}
        data = {'graphId': graph_id}
        
        response = requests.post(f'{self.base_url}/namespace/sparql', files=files, data=data)
        
        # Check if the response is JSON
        if response.headers.get('Content-Type') == 'application/json':
            return response.json()
        else:
            return {'status': 'TTL file uploaded', 'response': response.text}

    def get_all_namespaces(self):
        response = requests.get(f'{self.base_url}/namespace')
        if response.status_code == 200:
            try:
                namespaces = self.parse_rdf_xml(response.text)
                return {'status': 'Success', 'namespaces': namespaces}
            except ET.ParseError:
                return {'error': 'Invalid XML response'}
        else:
            response.raise_for_status()

    def parse_rdf_xml(self, xml_content):
        namespaces = []
        root = ET.fromstring(xml_content)
        for description in root.findall('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description'):
            title = description.find('{http://purl.org/dc/terms/}title')
            if title is not None:
                namespaces.append(title.text)
        return namespaces