from django.contrib import admin
from .models import Nfe, Cte, Item, Log, MemoriaIa, Cliente, ProdutoMap

@admin.register(ProdutoMap)
class ProdutoMapAdmin(admin.ModelAdmin):
    list_display = ('nome_produto', 'peso_unitario_kg', 'manual')
    search_fields = ('nome_produto',)
    list_filter = ('manual', 'peso_unitario_kg') # Filtro lateral
    list_editable = ('peso_unitario_kg',) # Permite editar direto na lista!
    
    # Ação rápida para marcar como "Peso Pendente"
    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.order_by('nome_produto')

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
    # Mostra: Produto | NF | Qtd Comercial (5 CX) | Peso Unit (3kg) | Peso Total
    list_display = ('produto', 'numero_nf', 'qtd_formatada', 'qtd_display', 'peso_estimado_total', 'vl_total')
    search_fields = ('produto',)

@admin.register(Log)
class LogAdmin(admin.ModelAdmin):
    list_display = ('data_hora', 'tipo_doc', 'status', 'arquivo')
    list_filter = ('status', 'tipo_doc')

@admin.register(MemoriaIa)
class MemoriaIaAdmin(admin.ModelAdmin):
    list_display = ('cfop', 'fluxo', 'tipo_definido')
    
@admin.register(Cliente)
class ClienteAdmin(admin.ModelAdmin):
    list_display = ('nome', 'cidade', 'uf', 'distancia_km', 'latitude', 'longitude')
    search_fields = ('nome', 'cpf_cnpj', 'cidade')
    list_filter = ('uf',)
    
    # Ação para atualizar Lat/Lon manualmente via Admin se necessário
    actions = ['atualizar_geolocalizacao']

    @admin.action(description='Atualizar Geolocalização e Distância (API)')
    def atualizar_geolocalizacao(self, request, queryset):
        from .utils import get_lat_lon, get_distancia_osrm, ORIGEM_PADRAO
        count = 0
        for cliente in queryset:
            # 1. Busca Lat/Lon
            lat, lon = get_lat_lon(cliente.endereco, cliente.bairro, cliente.cidade, cliente.uf, cliente.cep)
            if lat and lon:
                cliente.latitude = lat
                cliente.longitude = lon
                
                # 2. Calcula Distância
                dist = get_distancia_osrm(ORIGEM_PADRAO['lat'], ORIGEM_PADRAO['lon'], lat, lon)
                cliente.distancia_km = dist
                
                cliente.save()
                count += 1
        
        self.message_user(request, f"{count} clientes atualizados com sucesso!")