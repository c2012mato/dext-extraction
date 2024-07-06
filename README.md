# dext-extraction
This repo contains script that extracts data from https://app.dext.com and upload to google cloud bigquery

```mermaid
graph TD;
    style StartNode fill:#3498db,stroke:#333,stroke-width:2px;
    style EndNode fill:#3498db,stroke:#333,stroke-width:2px;
    style ProcessNode fill:#95a5a6,stroke:#333,stroke-width:2px;
    style DataNode fill:#e74c3c,stroke:#333,stroke-width:2px;
    style ExtractNode fill:#f39c12,stroke:#333,stroke-width:2px;
    
    A((Start: Login Session with OTP)) --> B[Extract Client List Data];
    B --> C[Randomly Divide into 10 Partitions];
    C --> D(Multithreaded Login Sessions with OTP);
    D --> E[Extract JSON Response Data from Cost Inbox];
    D --> F[Extract JSON Response Data from Cost Archived];
    F --> I[Extract JSON Response Data from Cost Archived History];
    D --> G[Extract JSON Response Data from Sales Inbox];
    D --> H[Extract JSON Response Data from Sales Archived];
    E --> J(Transform to Data Frame);
    F --> J;
    G --> J;
    H --> J;
    I --> J;
    J --> K((Upload to BigQuery));
    
    style A fill:#3498db,stroke:#333,stroke-width:2px;
    style B fill:#f39c12,stroke:#333,stroke-width:2px;
    style C fill:#95a5a6,stroke:#333,stroke-width:2px;
    style D fill:#95a5a6,stroke:#333,stroke-width:2px;
    style E fill:#f39c12,stroke:#333,stroke-width:2px;
    style F fill:#f39c12,stroke:#333,stroke-width:2px;
    style G fill:#f39c12,stroke:#333,stroke-width:2px;
    style H fill:#f39c12,stroke:#333,stroke-width:2px;
    style I fill:#f39c12,stroke:#333,stroke-width:2px;
    style J fill:#e74c3c,stroke:#333,stroke-width:2px;
    style K fill:#3498db,stroke:#333,stroke-width:2px;

