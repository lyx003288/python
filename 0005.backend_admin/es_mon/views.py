from django.shortcuts import render
from django.http import HttpResponse

from django.template import loader


def index(request):
    return render(request, 'es_mon/index.html')


def dashboard(request):
    return render(request, 'es_mon/dashboard.html')


def table(request):
    return render(request, 'es_mon/table.html')