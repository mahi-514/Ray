from django.urls import path
from read_app.views import ReadOperations, AbortExecution


urlpatterns = [
    path('readOperations/', ReadOperations.as_view(), name='readOperations'),
    path('abortExecution/', AbortExecution.as_view(), name='abortExecution'),
]
