from confluent_kafka import Producer
import datetime as dt
import json
import random
import time
import uuid

# Fonction pour générer une transaction aléatoire
def generate_transaction():
    transaction_types = ['achat', 'remboursement', 'transfert']
    payment_methods = ['carte_de_credit', 'especes', 'virement_bancaire', 'erreur']

    current_time = dt.datetime.now().isoformat()

    Villes = ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille", "Rennes", "Reims", "Le Havre", "Saint-Étienne", "Toulon", None]
    Rues = ["Rue de la République", "Rue de Paris", "rue Auguste Delaune", "Rue Gustave Courbet", "Rue de Luxembourg", "Rue Fontaine", "Rue Zinedine Zidane", "Rue de Bretagne", "Rue Marceaux", "Rue Gambetta", "Rue du Faubourg Saint-Antoine", "Rue de la Grande Armée", "Rue de la Villette", "Rue de la Pompe", "Rue Saint-Michel", None]

    transaction_data = {
        "id_transaction": str(uuid.uuid4()),
        "type_transaction": random.choice(transaction_types),
        "montant": round(random.uniform(10.0, 1000.0), 2),
        "devise": "USD",
        "date": current_time,
        "lieu": f"{random.choice(Rues)}, {random.choice(Villes)}",
        "moyen_paiement": random.choice(payment_methods),
        "details": {
            "produit": f"Produit{random.randint(1, 100)}",
            "quantite": random.randint(1, 10),
            "prix_unitaire": round(random.uniform(5.0, 200.0), 2)
        },
        "utilisateur": {
            "id_utilisateur": f"User{random.randint(1, 1000)}",
            "nom": f"Utilisateur{random.randint(1, 1000)}",
            "adresse": f"{random.randint(1, 1000)} {random.choice(Rues)}, {random.choice(Villes)}",
            "email": f"utilisateur{random.randint(1, 1000)}@example.com"
        }
    }

    return transaction_data

# Initialisation du producteur Kafka
producer = Producer({
    'bootstrap.servers': 'localhost:9092'  # Serveur Kafka
})

try:
    for _ in range(200):
        # Générer une transaction et sérialiser en JSON
        transaction = generate_transaction()
        transaction_json = json.dumps(transaction).encode('utf-8')  # Sérialisation manuelle en JSON
        
        # Envoyer la transaction
        producer.produce('transaction', value=transaction_json)
        
        # Assurer que le message est bien envoyé
        producer.flush()
        print(f"Transaction envoyée : {transaction}")

        # Pause entre les envois
        time.sleep(1)

finally:
    # S'assurer que tous les messages sont bien envoyés
    producer.flush()
    print("Fin de l'envoi des messages.")
    
