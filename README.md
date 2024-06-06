# Embedding task processing

Full documentation for SYNC embedding option task processing:
    - link: https://www.notion.so/Explanation-of-tables-and-workflows-7233c2a4109d4a2990dcc486017abaf9

Full documentation for ASYNC embedding option task processing:
    - link: https://www.notion.so/Async-embedding-Sender-2eee8437498b4ddf8413e8b250a2586b



## Goal
    - Embed text chunks.



### How to clone the git repository on local machine?
    1. Go to the following link: https://github.com/HumanDevIP/embedding
    2. Click the green "Code" button and copy the link to clone the repository.
    3. Open the terminal on your local machine and execute the following command:
        git clone git@github.com:HumanDevIP/embedding.git



### Description
    Project consist of 3 main parts. 
        Two of them located in src folder:
            1. Embedding Flask App
                - provide API endpoint for producing embeddings using different embedding models for further sending them for saving on Receiver side.
                    (read full documentation to get acquainted with management system)
                - support only hf embedding models.
            2. Sync and Async Senders
                - Sync sender work in pair with Embedding Flask App and resposible for prepareing the data need to embed.
                - Async sender work in pair specifically with TEI docker container.
        
        The third one(Receiver App) is in https://github.com/HumanDevIP/receiver .



### Project structure.
        1. Flask App.
            + location: /src/embeddings_app
                could be ran via docker environment as wel as without.
            
        2. Sync and Async Senders
            + location: /src/senders



### Setup Instructions
    Using Embedding Flask App:
        1. Prepare necessary data in postgres db need to be embedded:
            1. create a task row in mgmt.task table
            2. create necessaray text chunks need to embed in mgmt.embed_chunk table
            3. create rows which refers to mgmt.embed_chunk instances and task in mgmt.embed_task_chunk table.

        2.  Flask App:
                1. Specify the required creds in the local.env file of the flask application.
                    - location: src/embeddings_app
                2. After that, launch the Embedding Flask App docker container.
                    - command to run the docker container: docker compose up -d --build
                3. Now the docker container with Embedding Flask Application is ready to accept http requests.

                4. This flask app provide 2 api endpoints for producing embeddings:
                    1. embed batch of text chunks with further sending the embeddings to the Receiver service for saving. (GPU resources)
                    2. embed single text chunk. (CPU resources)
                5. All api endpoints described in SWAGGER documentation.
                    - link example: http://{ip}:{port}/apidocs
        
        3. Sync Sender:
            1. Install necessary dependecies from requirements.txt file.
                - command: pip install -r requirements.txt
            2. Specify the required creds in the dev.env file of the sync sender.
                    + location: src/senders/sync
            3. Run the process_embedding_task.py python script to extract necessary data from postgres db, prepare it and sending to the Embedding Flask App.
        
        !!!READ THE FULL DOCUMENTATION TO UNDERSTAND THE WHOLE DATA MANAGEMENT. WITHOUT RECEIVER FUNCTIONALITY WHOLE SYSTEM WON'T WORK!!!
    

    2. Using TEI Docker Container
        2. Prepare necessary data in postgres db need to be embedded:
            1. create a task row in mgmt.task table
            2. create necessaray text chunks need to embed in mgmt.embed_chunk table
            3. create rows which refers to mgmt.embed_chunk instances and task in mgmt.embed_task_chunk table.

        2. TEI Docker Container
            1. Find out what kind of gpu you have. (necessary for chosing what docker container is suitable for you.)
            2. list of supported models and TEI Docker Containers for different GPU cards: https://huggingface.co/docs/text-embeddings-inference/en/supported_models
            Choose the right one which suitable for your requirements and run.
                command example for RTX4000 and nomic embedding model: 
                    docker run --gpus all -e HUGGING_FACE_HUB_TOKEN=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 -p 8080:80 -v $PWD/data:/data --pull always ghcr.io/huggingface/text-embeddings-inference:89-1.2 --model-id nomic-ai/nomic-embed-text-v1
            
            After that TEI docker container is ready to accept http requests.

            All api endpoints described in Swagger documentation:
                link example: http://{ip}:{port}/docs/
        
        3. Async Sender
            1. Install necessary dependecies from requirements.txt file.
                - command: pip install -r requirements.txt
            2. Fill creds in dev.env file.
                - location: src/senders/async/
            3. Run the async_hf_embedding_service.py python script to extract necessary data from postgres db, prepare it and sending to the TEI Docker Container.
        
        !!!READ THE FULL DOCUMENTATION TO UNDERSTAND THE ENTIRE DATA MANAGEMENT PROCESS. THE SYSTEM WILL NOT FUNCTION PROPERLY WITHOUT THE RECEIVER FUNCTIONALITY!!!



### Credentials
All credentials could be found on google disk in keepass file for AI team.