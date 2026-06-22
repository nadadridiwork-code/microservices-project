# Mini-Projet : Architecture Microservices E-Commerce

Ce projet implémente une architecture microservices complète et découplée pour une plateforme e-commerce en utilisant **Node.js**. 

Il intègre des protocoles de communication variés (**REST, GraphQL, gRPC et Kafka**), et respecte le principe de bases de données indépendantes par service (**SQLite3**).

---

## 1. Schéma d'Architecture

Le schéma ci-dessous décrit les flux de communication :
- Le client communique avec l'**API Gateway** via **REST** et **GraphQL**.
- L'**API Gateway** communique avec les microservices en aval via **gRPC** (Protobuf / HTTP/2).
- L'**Order Service** interagit avec le **Product Service** via **gRPC** pour valider les prix et stocks.
- La communication asynchrone est orchestrée par **Kafka** pour découpler les traitements (notifications et stocks).

```mermaid
graph TD
    Client[Client / Interface de Test]
    
    subgraph API Gateway (Port 4000)
        Gateway[API Gateway Server]
        REST[Interface REST]
        GQL[Interface GraphQL]
    end

    subgraph Microservices Backend
        MS1[Product Service<br/>gRPC: 50081<br/>DB: SQLite3]
        MS2[Order Service<br/>gRPC: 50082<br/>DB: SQLite3]
        MS3[Customer Service<br/>gRPC: 50083<br/>DB: SQLite3]
    end

    subgraph Event Broker
        KafkaBroker[Kafka Broker<br/>Port 9092]
    end

    %% Client entrypoints
    Client -->|REST HTTP/1.1| REST
    Client -->|GraphQL HTTP/1.1| GQL

    %% Gateway to services gRPC
    Gateway -->|gRPC / Protobuf| MS1
    Gateway -->|gRPC / Protobuf| MS2
    Gateway -->|gRPC / Protobuf| MS3

    %% Order Service calling Product Service for verification
    MS2 -->|gRPC Check Stock/Price| MS1

    %% Kafka Events Flow
    MS2 -->|Publish: order-events| KafkaBroker
    KafkaBroker -->|Subscribe: order-events| MS1
    KafkaBroker -->|Subscribe: order-events| MS3
    
    MS1 -->|Publish: product-events| KafkaBroker
    KafkaBroker -->|Subscribe: product-events| MS3
```

---

## 2. Description des Composants et Bases de Données

Chaque microservice dispose de sa propre base de données isolée et de responsabilités logiques distinctes :

1. **API Gateway** (Port `4000`)
   - **Rôle** : Point d'entrée unique de l'application. Traduit les requêtes externes (REST et GraphQL) en appels gRPC vers les services en aval.
   - **Base de données** : Aucune (sans état).

2. **Product Service** (Port gRPC `50081`)
   - **Rôle** : Gère le catalogue produit, les prix et les niveaux de stock.
   - **Base de données** : **SQLite3** (`product-service/products.db`), table `products`.
   - **Comportement Kafka** : Consomme le topic `order-events` pour déduire automatiquement le stock des produits vendus. Publie sur le topic `product-events` (ex: alerte de stock bas).

3. **Order Service** (Port gRPC `50082`)
   - **Rôle** : Traite la création de commandes et le calcul des totaux.
   - **Base de données** : **SQLite3** (`order-service/orders.db`), tables `orders` et `order_items`.
   - **Comportement gRPC** : Agit en tant que client gRPC du `Product Service` pour vérifier la validité et le prix de chaque produit avant d'enregistrer la commande.
   - **Comportement Kafka** : Publie un événement `OrderCreated` sur le topic `order-events` dès qu'une commande est validée.

4. **Customer Service** (Port gRPC `50083`)
   - **Rôle** : Profils clients, cumul des points de fidélité et logs d'activité.
   - **Base de données** : **SQLite3** (`customer-service/customers.db`), tables `customers` et `activities`.
   - **Comportement Kafka** : Consomme `order-events` (pour attribuer des points de fidélité lors d'un achat) et `product-events` (pour enregistrer les alertes administrateur comme le stock bas).

---

## 3. Topics Kafka Utilisés

Le système intègre deux topics Kafka événementiels majeurs :

*   **`order-events`** :
    *   **Producteur** : `Order Service`
    *   **Consommateurs** : `Product Service` (déduction de stock) et `Customer Service` (attribution de points).
    *   **Événement principal** : `OrderCreated` (contient les détails de la commande, le client et les articles achetés).
*   **`product-events`** :
    *   **Producteur** : `Product Service`
    *   **Consommateurs** : `Customer Service` (historique administratif).
    *   **Événements principaux** : `ProductStockUpdated` et `ProductLowStock` (se déclenche lorsque le stock descend en dessous de 5 unités).

---

## 4. Documentation des Interfaces

### Fichiers Protobuf (gRPC)
Les contrats d'interface gRPC se situent dans [shared/protos/](file:///C:/Users/Dridi_Nada/.gemini/antigravity-ide/scratch/microservices-project/shared/protos/) :
*   `product.proto` : Gère le catalogue (`GetProduct`, `ListProducts`, `CreateProduct`, `UpdateStock`).
*   `order.proto` : Gère le cycle de commande (`CreateOrder`, `GetOrder`, `ListOrders`).
*   `customer.proto` : Gère les profils (`GetCustomer`, `CreateCustomer`, `GetActivityLogs`).

### Endpoints REST (API Gateway)

| Méthode | URL | Description | Payload Exemple |
| :--- | :--- | :--- | :--- |
| **GET** | `/api/products` | Récupérer tous les produits | *Aucun* |
| **GET** | `/api/products/:id` | Récupérer un produit par son ID | *Aucun* |
| **POST** | `/api/products` | Créer un produit | `{"name": "Clavier", "price": 89.9, "stock": 10}` |
| **PUT** | `/api/products/:id/stock` | Ajuster le stock d'un produit | `{"quantity_change": -3}` |
| **POST** | `/api/orders` | Passer une commande | `{"customer_id": "cust-001", "items": [{"product_id": "prod-001", "quantity": 1}]}` |
| **GET** | `/api/orders` | Récupérer toutes les commandes | *Aucun* |
| **GET** | `/api/orders/:id` | Récupérer les détails d'une commande | *Aucun* |
| **POST** | `/api/customers` | Créer un profil client | `{"name": "Alice", "email": "alice@gmail.com"}` |
| **GET** | `/api/customers/:id` | Obtenir le profil d'un client | *Aucun* |
| **GET** | `/api/customers/:id/activities` | Obtenir l'historique d'un client | *Aucun* |

---

### Schéma GraphQL

Le endpoint GraphQL est accessible à l'adresse `http://localhost:4000/graphql`.

#### Requêtes (Queries) Disponibles
```graphql
type Query {
  product(id: ID!): Product
  products: [Product!]!
  order(id: ID!): Order
  orders: [Order!]!
  customer(id: ID!): Customer
  customerActivities(customerId: ID!): [ActivityLog!]!
}
```

#### Mutations Disponibles
```graphql
type Mutation {
  createProduct(name: String!, price: Float!, stock: Int!, description: String): Product
  createOrder(customerId: ID!, items: [OrderItemInput!]!): Order
  createCustomer(name: String!, email: String!): Customer
}
```

---

## 5. Instructions d'Installation et d'Exécution

### Prérequis
*   **Node.js** (Version 18 ou supérieure recommandée)
*   **Java 11+** (Requis pour exécuter le broker Kafka en local)

### Installation
Clonez le dépôt, placez-vous à la racine du projet, et lancez la commande d'installation générale qui installe toutes les dépendances de chaque service :
```bash
npm run install:all
```

### Exécution (Kafka Local 4.2.0 + Services)
Ce projet intègre un véritable broker **Apache Kafka 4.2.0** en mode KRaft configuré pour fonctionner nativement sous Windows.

1. **Démarrer l'ensemble (Broker Kafka + Microservices + API Gateway)** :
   ```bash
   npm run start:all
   ```
   *Cette commande lance le broker Kafka (`npm run start:broker`), attend quelques secondes pour s'assurer qu'il est prêt, puis démarre l'API Gateway, le Product Service, l'Order Service et le Customer Service en parallèle via `concurrently`.*

2. **Lancer le scénario de test d'intégration automatisé** (dans un autre terminal) :
   ```bash
   npm run test:flow
   ```
   *Ce script simule un cycle d'achat complet en passant par REST et valide la mise à jour des stocks, l'enregistrement des commandes et l'attribution des points de fidélité en interrogeant la base de données de chaque service.*
