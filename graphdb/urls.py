from django.urls import path
from graphdb import views

urlpatterns = [
    path('namespaces/', views.namespace_list, name='namespace-list'),
    path('namespaces/create/', views.create_namespace, name='create-namespace'),
    path('namespaces/upload-ttl/', views.upload_ttl, name='upload-ttl'),
    path('blazegraph-connection/', views.set_blazegraph_connection, name='blazegraph-connection'),
    path('blazegraph/create/', views.create_blazegraph_database, name='create-blazegraph'),
]