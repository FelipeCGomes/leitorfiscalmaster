from django.db import models

class Nfe(models.Model):
    chave_nf = models.CharField(max_length=44, primary_key=True)
    data = models.DateField(null=True)
    numero_nf = models.CharField(max_length=20)
    emitente = models.CharField(max_length=255)
    destinatario = models.CharField(max_length=255)
    cnpj_emit = models.CharField(max_length=14)
    cnpj_dest = models.CharField(max_length=14, null=True)
    uf_dest = models.CharField(max_length=2, null=True)
    valor_nf = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    peso_bruto = models.DecimalField(max_digits=15, decimal_places=3, default=0)
    transportadora = models.CharField(max_length=255, null=True)
    cidade_origem = models.CharField(max_length=100)
    cidade_destino = models.CharField(max_length=100)
    mod_frete = models.CharField(max_length=10, null=True)
    cfop_predominante = models.CharField(max_length=10, null=True)
    tipo_operacao = models.CharField(max_length=50, null=True)
    qtd_itens = models.IntegerField(default=0)
    cep_origem = models.CharField(max_length=10, null=True)
    cep_destino = models.CharField(max_length=10, null=True)
    distancia = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    arquivo = models.CharField(max_length=255)

class Cte(models.Model):
    chave_cte_propria = models.CharField(max_length=44)
    chave_nf = models.CharField(max_length=44)
    data = models.DateField(null=True)
    numero_cte = models.CharField(max_length=20)
    emitente = models.CharField(max_length=255)
    cnpj_emit = models.CharField(max_length=14)
    remetente = models.CharField(max_length=255, null=True)
    destinatario = models.CharField(max_length=255, null=True)
    frete_valor = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    peso_kg = models.DecimalField(max_digits=15, decimal_places=3, default=0)
    numero_nf_cte = models.CharField(max_length=20, null=True)
    cidade_origem = models.CharField(max_length=100)
    cidade_destino = models.CharField(max_length=100)
    pedagio_valor = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    chave_ref_cte = models.CharField(max_length=44, null=True, blank=True)
    tp_cte = models.CharField(max_length=10, default='0')
    arquivo = models.CharField(max_length=255)
    etapa_manual = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        unique_together = ('chave_cte_propria', 'chave_nf')

class Item(models.Model):
    chave_nf = models.CharField(max_length=44)
    numero_nf = models.CharField(max_length=20)
    emitente = models.CharField(max_length=255)
    item_num = models.CharField(max_length=10)
    produto = models.CharField(max_length=255)
    ncm = models.CharField(max_length=20, null=True)
    cfop = models.CharField(max_length=10, null=True)
    unidade = models.CharField(max_length=10, null=True)
    qtd_display = models.CharField(max_length=20)
    qtd_float = models.DecimalField(max_digits=15, decimal_places=4)
    vl_total = models.DecimalField(max_digits=15, decimal_places=2)
    arquivo = models.CharField(max_length=255)

class Meta:
        # Garante que não existam dois itens com o mesmo número na mesma nota
        unique_together = ('chave_nf', 'item_num')

class MemoriaIa(models.Model):
    cfop = models.CharField(max_length=10)
    fluxo = models.CharField(max_length=50)
    tipo_definido = models.CharField(max_length=50)

    class Meta:
        unique_together = ('cfop', 'fluxo')

class Log(models.Model):
    data_hora = models.DateTimeField(auto_now_add=True)
    arquivo = models.CharField(max_length=255)
    tipo_doc = models.CharField(max_length=50)
    status = models.CharField(max_length=50)
    mensagem = models.TextField()