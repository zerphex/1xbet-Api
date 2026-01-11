API 1xBet - Solution Complète

API complète pour les événements sportifs en temps réel avec statistiques détaillées, cotes en direct, authentification sécurisée et streaming WebSocket.

🚀 Fonctionnalités Principales

· 📊 Statistiques Temps Réel : Données détaillées par catégorie (tirs, possession, discipline, etc.)
· ⚽ Événements Sportifs : Matchs en direct, à venir et terminés
· 📈 Cotes en Direct : Mises à jour automatiques des cotes
· 🔐 Authentification Sécurisée : JWT tokens avec permissions granulaires
· 🔗 WebSocket : Streaming temps réel des événements
· 🏆 Multi-sports : Football, basketball, tennis, e-sports, etc.
· 📱 API RESTful : Endpoints bien documentés

🛠️ Technologies Utilisées

· Backend : Python, FastAPI, SQLAlchemy, Redis
· Authentification : JWT, OAuth2
· Base de Données : PostgreSQL
· Cache : Redis
· WebSocket : WebSockets natifs
· Tests : Pytest, Requests

📦 Installation Rapide

```bash
# 1. Cloner le projet
git clone <repository-url>
cd 1xbet-api

# 2. Créer l'environnement virtuel
python -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Configurer les variables d'environnement
cp .env.example .env
# Éditer .env avec vos configurations

# 5. Lancer les services
docker-compose up -d

# 6. Démarrer l'API
python main.py
```

🔧 Configuration

```env
# .env
DATABASE_URL=postgresql://user:password@localhost:5432/1xbet
REDIS_URL=redis://localhost:6379/0
SECRET_KEY=votre-clé-secrète
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
```

📡 Endpoints Principaux

Public

· GET /health - Vérification du statut
· GET /api/v1/sports - Liste des sports
· GET /api/v1/events - Événements avec filtres
· GET /api/v1/events/{id} - Détails d'un événement

Authentifié

· GET /api/v1/events/{id}/stats - Statistiques détaillées
· GET /api/v1/events/{id}/odds - Cotes en direct
· GET /api/v1/protected/* - Données protégées

WebSocket

· ws://localhost:8000/ws/events - Streaming événements
· ws://localhost:8000/ws/events/{id} - Streaming spécifique

🧪 Tests

```bash
# Lancer tous les tests
python -m pytest

# Tests avec couverture
python -m pytest --cov=.

# Test de performance
python debug_final4.py
```

📊 Structure du Projet

```
1xbet-api/
├── app/
│   ├── api/           # Endpoints API
│   ├── core/          # Configuration et sécurité
│   ├── models/        # Modèles de données
│   ├── services/      # Logique métier
│   └── utils/         # Utilitaires
├── tests/             # Tests unitaires
├── docker-compose.yml # Services Docker
├── requirements.txt   # Dépendances
└── README.md          # Documentation
```

🔐 Authentification

```bash
# Obtenir un token
curl -X POST "http://localhost:8000/api/v1/auth/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=admin123"

# Utiliser le token
curl -H "Authorization: Bearer <token>" \
  "http://localhost:8000/api/v1/protected/events"
```

🌐 WebSocket Exemple

```javascript
const ws = new WebSocket('ws://localhost:8000/ws/events?token=<token>');

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    console.log('Événement mis à jour:', data);
};
```

📈 Monitoring

· Statistiques API : GET /api/v1/stats
· Logs : Fichiers journalisés avec rotation
· Métriques : Prometheus metrics disponibles
· Audit : Traces des requêtes authentifiées

🐛 Dépannage

Problèmes Courants

1. Base de données non accessible
   ```bash
   docker-compose restart postgres
   ```
2. Token JWT expiré
   ```bash
   # Rafraîchir le token
   curl -X POST /api/v1/auth/refresh
   ```
3. WebSocket non connecté
   ```bash
   # Vérifier le token
   curl -H "Authorization: Bearer <token>" /api/v1/auth/profile
   ```

📝 Licence

Ce projet est sous licence MIT. Voir le fichier LICENSE pour plus de détails.

🤝 Contribution

1. Fork le projet
2. Créer une branche (git checkout -b feature/AmazingFeature)
3. Commit les changements (git commit -m 'Add AmazingFeature')
4. Push vers la branche (git push origin feature/AmazingFeature)
5. Ouvrir une Pull Request

📞 Support

Pour toute question ou problème :

· Issues GitHub : [Lien vers les issues]
· Email : support@example.com
· Documentation : /docs (Swagger UI)

---

Version : 5.2.0
Dernière mise à jour : Janvier 2024
Statut : 🟢 Production Ready
