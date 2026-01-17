from django.contrib import admin
from .models import Nfe, Cte, Item, Log, MemoriaIa

@admin.register(Nfe)
class NfeAdmin(admin.ModelAdmin):
    list_display = ('chave_nf', 'numero_nf', 'emitente', 'valor_nf', 'data')
    search_fields = ('numero_nf', 'emitente', 'destinatario')
    list_filter = ('data', 'uf_dest')

@admin.register(Cte)
class CteAdmin(admin.ModelAdmin):
    list_display = ('numero_cte', 'emitente', 'frete_valor', 'data')
    search_fields = ('numero_cte', 'chave_cte_propria')

@admin.register(Item)
class ItemAdmin(admin.ModelAdmin):
    list_display = ('produto', 'numero_nf', 'qtd_display', 'vl_total')
    search_fields = ('produto',)

@admin.register(Log)
class LogAdmin(admin.ModelAdmin):
    list_display = ('data_hora', 'tipo_doc', 'status', 'arquivo')
    list_filter = ('status', 'tipo_doc')

@admin.register(MemoriaIa)
class MemoriaIaAdmin(admin.ModelAdmin):
    list_display = ('cfop', 'fluxo', 'tipo_definido')