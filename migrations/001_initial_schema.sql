CREATE TABLE users (
    user_id INT PRIMARY KEY,
    username VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    is_active BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE chats (
    chat_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255),
    client_name VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE messages (
    message_id VARCHAR(255) PRIMARY KEY,
    chat_id VARCHAR(255) NOT NULL REFERENCES chats(chat_id) ON DELETE CASCADE,
    text TEXT,
    is_from_company BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE chat_reports (
    chat_id VARCHAR(255) NOT NULL REFERENCES chats(chat_id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    chat_title VARCHAR(255),
    client_name VARCHAR(255),
    chat_created_at TIMESTAMP WITH TIME ZONE,
    chat_updated_at TIMESTAMP WITH TIME ZONE,
    total_messages INT DEFAULT 0,
    company_messages INT DEFAULT 0,
    client_messages INT DEFAULT 0,
    tonality_grade VARCHAR(255),
    tonality_comment TEXT,
    professionalism_grade VARCHAR(255),
    professionalism_comment TEXT,
    clarity_grade VARCHAR(255),
    clarity_comment TEXT,
    problem_solving_grade VARCHAR(255),
    problem_solving_comment TEXT,
    objection_handling_grade VARCHAR(255),
    objection_handling_comment TEXT,
    closure_grade VARCHAR(255),
    closure_comment TEXT,
    summary TEXT,
    recommendations TEXT
); 

ALTER TABLE chat_reports
ADD CONSTRAINT chat_reports_chat_id_unique UNIQUE (chat_id);
        
  
    
    
    
    
    
    
    
    