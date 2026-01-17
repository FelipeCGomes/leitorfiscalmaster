from django.urls import path
from django.contrib.auth import views as auth_views
from . import views

urlpatterns = [
    # Rotas da Aplicação
    path('', views.dashboard, name='dashboard'),
    path('upload/', views.upload_files, name='upload'),
    path('analise/', views.analise, name='analise'),

    # Rotas de Autenticação
    path('login/', auth_views.LoginView.as_view(template_name='core/login.html'), name='login'),
    path('logout/', auth_views.LogoutView.as_view(next_page='login'), name='logout'),
]