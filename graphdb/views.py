from django.shortcuts import render
import requests
import json
from rest_framework import status
from rest_framework.response import Response
from rest_framework.decorators import api_view
from .models import Namespace
from .serializers import NamespaceSerializer
from .services.blazegraph_service import BlazegraphService
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
import subprocess
import os
from django.conf import settings


blazegraph_service = BlazegraphService(base_url=settings.BLAZEGRAPH_URL)

@api_view(['GET'])
def namespace_list(request):
    try:
        result = blazegraph_service.get_all_namespaces()
        if 'error' in result:
            return Response(result, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response(result, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
def create_namespace(request):
    
    name = request.data.get('name')
    try:
        result = blazegraph_service.create_namespace(name)
        # Save the namespace to the Django database
        namespace, created = Namespace.objects.get_or_create(name=name)
        if created:
            return Response(result, status=status.HTTP_201_CREATED)
        else:
            return Response({'error': 'Namespace already exists'}, status=status.HTTP_400_BAD_REQUEST)
    except requests.exceptions.RequestException as e:
        return Response({'error': str(e)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)

@api_view(['POST'])
def upload_ttl(request):
    print("data..", request.data)
    ttl_file = request.FILES.get('file')
    filename = request.data.get('filename')
    graph_id = request.data.get('graphId')

    if not ttl_file:
        return Response({'error': 'No file uploaded'}, status=status.HTTP_400_BAD_REQUEST)
    
    try:
        result = blazegraph_service.upload_ttl(ttl_file, filename, graph_id)
        return Response(result, status=status.HTTP_200_OK)
    except ConnectionError as e:
        return Response({'error': str(e)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)

@api_view(['POST'])
def set_blazegraph_connection(request):
    
    ip = request.data.get('ipAddress')
    port = request.data.get('port')
    if not ip or not port:
        return Response({'error': 'IP and port are required'}, status=status.HTTP_400_BAD_REQUEST)
    
    result = blazegraph_service.set_connection(ip, port)
    return Response(result)

@api_view(['POST'])
def create_blazegraph_database(request):
    
    installation_path = request.data.get('installationPath')
    port = request.data.get('port')
    min_memory = request.data.get('minMemory', '1g')
    max_memory = request.data.get('maxMemory', '2g')

    if not installation_path or not port:
        return Response({'error': 'Installation path and port are required'}, status=status.HTTP_400_BAD_REQUEST)

    try:
        # Ensure the installation directory exists
        os.makedirs(installation_path, exist_ok=True)

        # Create a properties file for the new database
        properties_file = os.path.join(installation_path, 'blazegraph.properties')
        with open(properties_file, 'w') as f:
            f.write(f"""
            com.bigdata.journal.AbstractJournal.file=blazegraph.jnl
            com.bigdata.journal.AbstractJournal.bufferMode=DiskRW
            com.bigdata.service.AbstractTransactionService.minReleaseAge=1
            com.bigdata.journal.Journal.groupCommit=true
            com.bigdata.btree.writeRetentionQueue.capacity=4000
            com.bigdata.btree.BTree.branchingFactor=128
            com.bigdata.journal.AbstractJournal.initialExtent=209715200
            com.bigdata.journal.AbstractJournal.maximumExtent=209715200
            jetty.port={port}
            """)

        # Start the Blazegraph server
        cmd = [
            'java',
            f'-Xms{min_memory}',
            f'-Xmx{max_memory}',
            '-server',
            '-jar',
            'blazegraph.jar',
            f'-Djetty.port={port}',
            f'-Dcom.bigdata.journal.AbstractJournal.file={os.path.join(installation_path, "blazegraph.jnl")}'
        ]

        process = subprocess.Popen(cmd, cwd=installation_path)

        # Update the BlazegraphService connection
        blazegraph_service.set_connection('localhost', port)

        return Response({
            'status': 'Blazegraph database created and started successfully',
            'pid': process.pid,
            'port': port,
            'installation_path': installation_path
        }, status=status.HTTP_201_CREATED)

    except Exception as e:
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)