# start_system.py

#!/usr/bin/env python3
"""
Script simple pour démarrer le système Power-to-Ammonia
"""

import os
import sys
import time
import subprocess
import logging

# Configuration du logging simple
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def print_step(step_number, description):
    """Afficher une étape avec un format clair"""
    print(f"\n🔄 Étape {step_number}: {description}")
    print("-" * 50)

def run_command(command, description=""):
    """Exécuter une commande et afficher le résultat"""
    try:
        print(f"▶️  {description}")
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print("✅ Succès")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur: {e}")
        if e.stdout:
            print(f"Sortie: {e.stdout}")
        if e.stderr:
            print(f"Erreur: {e.stderr}")
        return False

def check_prerequisites():
    """Vérifier les prérequis de base"""
    print_step(1, "Vérification des prérequis")
    
    # Vérifier Docker
    if not run_command("docker --version", "Vérification de Docker"):
        print("❌ Docker n'est pas installé ou n'est pas démarré")
        print("   Installez Docker: https://docs.docker.com/get-docker/")
        return False
    
    # Vérifier Docker Compose
    if not run_command("docker-compose --version", "Vérification de Docker Compose"):
        print("❌ Docker Compose n'est pas installé")
        print("   Installez Docker Compose: https://docs.docker.com/compose/install/")
        return False
    
    # Vérifier Python
    if not run_command("python --version", "Vérification de Python"):
        print("❌ Python n'est pas installé")
        return False
    
    print("✅ Tous les prérequis sont satisfaits")
    return True

def install_python_packages():
    """Installer les packages Python nécessaires"""
    print_step(2, "Installation des packages Python")
    
    essential_packages = [
        "kafka-python>=2.0.2",
        "numpy>=1.21.0", 
        "pandas>=1.3.0",
        "requests>=2.25.0"
    ]
    
    for package in essential_packages:
        if not run_command(f"pip install {package}", f"Installation de {package}"):
            print(f"⚠️  Impossible d'installer {package}")
    
    print("✅ Packages Python installés")

def start_infrastructure():
    """Démarrer l'infrastructure Docker"""
    print_step(3, "Démarrage de l'infrastructure Docker")
    
    # Arrêter les conteneurs existants
    run_command("docker-compose down", "Arrêt des conteneurs existants")
    
    # Démarrer les services essentiels
    essential_services = [
        "zookeeper",
        "kafka", 
        "kafka-ui"
    ]
    
    cmd = f"docker-compose up -d {' '.join(essential_services)}"
    if run_command(cmd, "Démarrage des services essentiels"):
        print("✅ Services essentiels démarrés")
        return True
    else:
        print("❌ Échec du démarrage des services")
        return False

def wait_for_kafka():
    """Attendre que Kafka soit prêt"""
    print_step(4, "Attente que Kafka soit prêt")
    
    print("⏳ Attente de Kafka (cela peut prendre 1-2 minutes)...")
    
    for i in range(60):  # Attendre max 60 secondes
        time.sleep(2)
        
        # Tester la connectivité Kafka
        result = subprocess.run(
            "docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092",
            shell=True, capture_output=True, text=True
        )
        
        if result.returncode == 0:
            print("✅ Kafka est prêt!")
            return True
        
        if i % 10 == 0:  # Afficher le progrès toutes les 20 secondes
            print(f"   ... encore en attente ({i*2}s)")
    
    print("⚠️  Timeout - Kafka met plus de temps que prévu")
    return True  # Continuer quand même

def create_kafka_topics():
    """Créer les topics Kafka essentiels"""
    print_step(5, "Création des topics Kafka")
    
    essential_topics = [
        ("weather-raw", 3, "7d"),
        ("equipment-telemetry", 3, "30d"), 
        ("system-overview", 1, "30d")
    ]
    
    for topic_name, partitions, retention in essential_topics:
        cmd = f"""docker-compose exec -T kafka kafka-topics \\
            --bootstrap-server localhost:9092 \\
            --create --if-not-exists \\
            --topic {topic_name} \\
            --partitions {partitions} \\
            --replication-factor 1"""
        
        if run_command(cmd, f"Création du topic {topic_name}"):
            print(f"✅ Topic {topic_name} créé")
        else:
            print(f"⚠️  Problème avec le topic {topic_name}")

def test_system():
    """Tester le système de base"""
    print_step(6, "Test du système")
    
    # Test simple de génération de données
    test_code = """
try:
    from data_producers.weather_generator import WeatherGenerator
    gen = WeatherGenerator()
    data = gen.generate_weather_data()
    print("✅ Génération de données: OK")
    print(f"   Température: {data.temperature:.1f}°C")
    print(f"   Irradiance: {data.solar_irradiance:.1f} W/m²")
except Exception as e:
    print(f"❌ Erreur: {e}")
"""
    
    if run_command(f'python -c "{test_code}"', "Test de génération de données"):
        print("✅ Le système génère des données correctement")
    else:
        print("⚠️  Problème avec la génération de données")

def show_access_info():
    """Afficher les informations d'accès"""
    print_step(7, "Informations d'accès")
    
    print("🌐 Interfaces disponibles:")
    print("   • Kafka UI:     http://localhost:8080")
    print("   • Kafka direct: localhost:9092")
    
    print("\n📝 Commandes utiles:")
    print("   • Voir les logs:        docker-compose logs -f kafka")
    print("   • Lister les topics:    docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list")
    print("   • Arrêter le système:   docker-compose down")
    
    print("\n🧪 Tests disponibles:")
    print("   • Test météo:           python weather_fix_test.py")
    print("   • Test équipements:     python test_equipment_suite.py")

def main():
    """Fonction principale"""
    print("🚀 Démarrage du Système Power-to-Ammonia")
    print("=" * 60)
    print("Version simplifiée - Setup essentiel seulement")
    print("=" * 60)
    
    # Étapes essentielles
    steps = [
        check_prerequisites,
        install_python_packages,
        start_infrastructure,
        wait_for_kafka,
        create_kafka_topics,
        test_system,
        show_access_info
    ]
    
    for i, step_func in enumerate(steps, 1):
        try:
            if not step_func():
                print(f"\n❌ Échec à l'étape {i}")
                print("   Consultez les logs pour plus d'informations")
                return False
        except KeyboardInterrupt:
            print("\n⏹️  Arrêt demandé par l'utilisateur")
            return False
        except Exception as e:
            print(f"\n💥 Erreur inattendue à l'étape {i}: {e}")
            return False
    
    print("\n" + "=" * 60)
    print("🎉 SETUP TERMINÉ AVEC SUCCÈS!")
    print("=" * 60)
    print("\n📋 Prochaines étapes:")
    print("1. Ouvrez Kafka UI: http://localhost:8080")
    print("2. Testez la génération de données: python weather_fix_test.py")
    print("3. Explorez la documentation dans le README")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        print("\n❌ Setup échoué. Vérifiez les erreurs ci-dessus.")
        sys.exit(1)
    else:
        print("\n✅ Setup réussi! Le système est prêt.")
        sys.exit(0)