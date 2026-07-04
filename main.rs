use axum::{
    extract::{Path, State, Query},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json,
    Router,
    response::sse::{Event, Sse},
    extract::ConnectInfo,
};

use futures_util::stream::{self, Stream};
use std::{convert::Infallible, time::Duration};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::compression::CompressionLayer;
use tokio::sync::RwLock;
use std::collections::HashMap;
use sqlx::sqlite::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, FromRow};

use tokio::time::sleep;
// use std::process::Command; //incompatible with Onion Omega2 Pro without SDK
use std::fs;
use std::ffi::CString;
use nix::unistd::{fork, ForkResult, execvp};

#[derive(Clone)]
struct AppState {
    pool: SqlitePool,
    handle: String,
    tx: broadcast::Sender<String>,
    current_headline: Arc<RwLock<String>>,
    users: Vec<User>,
    profiles: Vec<Profile>,
    skills: Vec<Vec<Skill>>,
    experiences: Vec<Vec<Experience>>,
    projects: Vec<Vec<Project>>,
    analytics_matrix: Vec<Analytics>,
    note_versions: Arc<RwLock<HashMap<String, u64>>>,
}

#[derive(Clone, Serialize, Deserialize, FromRow)]
pub struct User {
    pub profile_handle: String,
    pub password: String,
}

#[derive(Clone, Serialize, Deserialize, FromRow)]
pub struct Profile {
    pub picture: String,
    pub handle: String, // Made handle the primary identifier
    pub name: String,
    pub title: String,
    pub location: String,
    pub summary: String,
}

#[derive(Clone, Serialize, Deserialize, FromRow)]
pub struct Skill {
    pub id: String,
    pub profile_handle: String,
    pub name: String,
    pub category: String,
    pub score: u8,
    #[sqlx(json)] // Tells SQLx to parse this text column as JSON
    pub links: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize, FromRow)]
pub struct Experience {
    pub id: String,
    pub profile_handle: String,
    pub role: String,
    pub organization: String,
    pub years: f32,
    pub summary: String,
    #[sqlx(json)]
    pub achievements: Vec<String>,
    #[sqlx(json)]
    pub skills: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize, FromRow)]
pub struct Project {
    pub id: String,
    pub profile_handle: String,
    pub name: String,
    pub impact: u8,
    pub description: String,
    #[sqlx(json)]
    pub technologies: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize, FromRow)]
pub struct Analytics {
    pub id: String,
    pub leadership: u32,
    pub technical_depth: u32,
    pub automation_index: u32,
    pub transferability: u32,
    pub innovation: u32,
    pub neural_load: u32,
}

#[derive(Serialize)]
struct Dashboard {
    profiles: Vec<Profile>,
    skills: Vec<Vec<Skill>>,
    experiences: Vec<Vec<Experience>>,
    projects: Vec<Vec<Project>>,
    analytics: Vec<Analytics>,
}

/// A master struct to accept the entire payload at once
#[derive(Serialize, Deserialize)]
struct FullResumeUplink {
    profile: Profile,
    skills: Vec<Skill>,
    experiences: Vec<Experience>,
    projects: Vec<Project>,
    analytics: Analytics,
}

// --- CONCURRENCY SCHEMAS ---
#[derive(Serialize, Deserialize)]
struct NotesPayload {
    text: String,
    version: u64,
}

#[derive(Deserialize)]
struct SaveNotesRequest {
    text: String,
    version: u64,
}

#[derive(Serialize)]
struct SaveNotesResponse {
    #[serde(rename = "newVersion")]
    new_version: u64,
}

#[derive(Serialize)]
struct VersionResponse {
    version: u64,
}

#[derive(Deserialize)]
struct DashboardQuery {
    handle: String,
}

#[derive(Deserialize)]
pub struct SearchParams {
    q: String,
}

/// Fully structured object matching the database layout requested by the frontend
#[derive(Serialize, FromRow)]
pub struct SearchProfile {
    handle: String,
    name: Option<String>,
    title: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, FromRow)]
struct SubProjectQuery {
    project_id: String,
    profile_handle: String,
}

#[derive(Clone, Serialize, Deserialize, FromRow)]
struct NewSubProjectQuery {
    project_id: String,
    project_name : String,
    profile_handle: String,
    subproject_name: String,
    subproject_category: String,
}

#[derive(Clone, Serialize, Deserialize, FromRow)]
pub struct SubProject {
    pub id: i32,
    pub project_id: String,
    pub project_name: String,
    pub profile_handle: String,
    pub subproject_name: String,
    pub subproject_category: String,
    pub display_order: i32,
}

#[derive(Clone, Serialize, Deserialize, FromRow)]
struct EditQuery {
    profile_handle: String,
}

// --- SEARCH BOX ---

pub async fn search_profiles(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> impl IntoResponse {
    // Escape target parameters for standard SQL partial matching
    let search_pattern = format!("%{}%", params.q.replace('@', "")); 
    let pool = &state.pool;

    // Use the generic runtime function query_as::<_, StructName>
    let query_result = sqlx::query_as::<_, SearchProfile>(
        r#"
        SELECT handle, name, title
        FROM profiles 
        WHERE handle LIKE ? OR name LIKE ? OR title LIKE ?
        LIMIT 2
        "#,
    )
    .bind(&search_pattern) // Explicitly bind each query variable sequentially
    .bind(&search_pattern)
    .bind(&search_pattern)
    .fetch_all(pool)
    .await;

    match query_result {
        Ok(records) => {
            // Records are now an array of SearchProfile instances, safe for JSON translation
            (StatusCode::OK, Json(records)).into_response()
        }
        Err(e) => {
            eprintln!("CRITICAL ERROR // SQLite search sequence failure: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "REGISTRY CORE FAIL").into_response()
        }
    }
}


// --- DATABASE ---

/// Initializes the database schema if it does not already exist.
/// Initializes the database schema based on the AppState structs.
pub async fn init_db(pool: &sqlx::SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS profiles (
            handle TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            title TEXT NOT NULL,
            location TEXT NOT NULL,
            summary TEXT NOT NULL,
            picture TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS skills (
            id TEXT PRIMARY KEY,
            profile_handle TEXT NOT NULL,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            score INTEGER NOT NULL, /* Maps to u8 */
            links TEXT NOT NULL /* Maps to #[sqlx(json)] Vec<String> */
        );

        CREATE TABLE IF NOT EXISTS experiences (
            id TEXT PRIMARY KEY,
            profile_handle TEXT NOT NULL,
            role TEXT NOT NULL,
            organization TEXT NOT NULL,
            years REAL NOT NULL, /* Maps to f32 */
            summary TEXT NOT NULL,
            achievements TEXT NOT NULL, /* Maps to #[sqlx(json)] Vec<String> */
            skills TEXT NOT NULL /* Maps to #[sqlx(json)] Vec<String> */
        );

        CREATE TABLE IF NOT EXISTS projects (
            id TEXT PRIMARY KEY,
            profile_handle TEXT NOT NULL,
            name TEXT NOT NULL,
            impact INTEGER NOT NULL, /* Maps to u8 */
            description TEXT NOT NULL,
            technologies TEXT NOT NULL /* Maps to #[sqlx(json)] Vec<String> */
        );

        CREATE TABLE IF NOT EXISTS analytics (
            id TEXT PRIMARY KEY,
            leadership INTEGER NOT NULL, /* Maps to u32 */
            technical_depth INTEGER NOT NULL, /* Maps to u32 */
            automation_index INTEGER NOT NULL, /* Maps to u32 */
            transferability INTEGER NOT NULL, /* Maps to u32 */
            innovation INTEGER NOT NULL, /* Maps to u32 */
            neural_load INTEGER NOT NULL /* Maps to u32 */
        );

        CREATE TABLE IF NOT EXISTS sub_projects (
            id SERIAL PRIMARY KEY,
            project_id TEXT NOT NULL,
            project_name TEXT NOT NULL,
            profile_handle TEXT NOT NULL,
            subproject_name TEXT NOT NULL,
            subproject_category TEXT NOT NULL,
            display_order INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Foreign Key Constraints linking to your parent table
            CONSTRAINT fk_parent_project 
                FOREIGN KEY (project_id) 
                REFERENCES projects(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS users (
            profile_handle TEXT PRIMARY KEY,
            password TEXT NOT NULL
        );

        "#
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn save_user(pool: &SqlitePool, user: &User) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO users (profile_handle, password) 
        VALUES (?, ?)
        ON CONFLICT(profile_handle) DO UPDATE SET 
            password =  excluded.password
        "#,
    )
    .bind(&user.profile_handle)
    .bind(&user.password)
    .execute(pool)
    .await?;

    Ok(())
}

/// Saves a Profile. Uses `handle` as the unique identifier.
pub async fn save_profile(pool: &SqlitePool, profile: &Profile) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO profiles (handle, name, title, location, summary, picture) 
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(handle) DO UPDATE SET 
            name = excluded.name,
            title = excluded.title,
            location = excluded.location,
            summary = excluded.summary,
            picture = excluded.picture
        "#,
    )
    .bind(&profile.handle)
    .bind(&profile.name)
    .bind(&profile.title)
    .bind(&profile.location)
    .bind(&profile.summary)
    .bind(&profile.picture)
    .execute(pool)
    .await?;

    Ok(())
}

/// Saves a Skill. Serializes `links` to JSON.
pub async fn save_skill(pool: &SqlitePool, handle: &str, skill: &Skill) -> Result<(), sqlx::Error> {
    let links_json = serde_json::to_string(&skill.links)
        .map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;

    sqlx::query(
        r#"
        INSERT INTO skills (id, profile_handle, name, category, score, links) 
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            profile_handle = excluded.profile_handle, 
            name = excluded.name,
            category = excluded.category,
            score = excluded.score,
            links = excluded.links
        "#,
    )
    .bind(&skill.id)
    .bind(handle)
    .bind(&skill.name)
    .bind(&skill.category)
    .bind(skill.score)
    .bind(links_json)
    .execute(pool)
    .await?;

    Ok(())
}

/// Saves an Experience. Serializes `achievements` and `skills` to JSON.
pub async fn save_experience(pool: &SqlitePool, handle: &str, exp: &Experience) -> Result<(), sqlx::Error> {
    let achievements_json = serde_json::to_string(&exp.achievements)
        .map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;
    let skills_json = serde_json::to_string(&exp.skills)
        .map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;

    sqlx::query(
        r#"
        INSERT INTO experiences (id, profile_handle, role, organization, years, summary, achievements, skills) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET 
            profile_handle = excluded.profile_handle, 
            role = excluded.role,
            organization = excluded.organization,
            years = excluded.years,
            summary = excluded.summary,
            achievements = excluded.achievements,
            skills = excluded.skills
        "#,
    )
    .bind(&exp.id)
    .bind(handle)
    .bind(&exp.role)
    .bind(&exp.organization)
    .bind(exp.years)
    .bind(&exp.summary)
    .bind(achievements_json)
    .bind(skills_json)
    .execute(pool)
    .await?;

    Ok(())
}

/// Saves a Project. Serializes `technologies` to JSON.
pub async fn save_project(pool: &SqlitePool, handle: &str, project: &Project) -> Result<(), sqlx::Error> {
    let tech_json = serde_json::to_string(&project.technologies)
        .map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;

    sqlx::query(
        r#"
        INSERT INTO projects (id, profile_handle, name, impact, description, technologies) 
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET 
            profile_handle = excluded.profile_handle, 
            name = excluded.name,
            impact = excluded.impact,
            description = excluded.description,
            technologies = excluded.technologies
        "#,
    )
    .bind(&project.id)
    .bind(handle)
    .bind(&project.name)
    .bind(project.impact)
    .bind(&project.description)
    .bind(tech_json)
    .execute(pool)
    .await?;

    Ok(())
}




/// Saves Analytics. 
pub async fn save_analytics(pool: &SqlitePool, analytics: &Analytics) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO analytics (
            id, leadership, technical_depth, automation_index, 
            transferability, innovation, neural_load
        ) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            leadership = excluded.leadership,
            technical_depth = excluded.technical_depth,
            automation_index = excluded.automation_index,
            transferability = excluded.transferability,
            innovation = excluded.innovation,
            neural_load = excluded.neural_load
        "#,
    )
    .bind(&analytics.id)
    .bind(&analytics.leadership)
    .bind(&analytics.technical_depth)
    .bind(&analytics.automation_index)
    .bind(&analytics.transferability)
    .bind(&analytics.innovation)
    .bind(&analytics.neural_load)
    .execute(pool)
    .await?;

    Ok(())
}

async fn get_user_password(pool: &SqlitePool, profile_handle: &str) -> Result<User, sqlx::Error> {
    let user = sqlx::query_as::<_, User>("SELECT * FROM users WHERE profile_handle = ?")
        .bind(profile_handle)
        .fetch_one(pool)
        .await?;
    Ok(user)
}

async fn fetch_dashboard_for_handle(pool: &SqlitePool, handle: &str) -> Result<Dashboard, sqlx::Error> {
    // 1. Fetch only the profile associated with this handle
    let profiles = sqlx::query_as::<_, Profile>("SELECT * FROM profiles")// WHERE handle = ?")
        //.bind(handle)
        .fetch_all(pool) //fetch_one
        .await?;

    // 2. Fetch Skills filtered by profile_handle
    let skills = sqlx::query_as::<_, Skill>("SELECT * FROM skills WHERE profile_handle = ?")
        .bind(handle)
        .fetch_all(pool)
        .await?;

    // 3. Fetch Experiences filtered by profile_handle
    let experiences = sqlx::query_as::<_, Experience>("SELECT * FROM experiences WHERE profile_handle = ?")
        .bind(handle)
        .fetch_all(pool)
        .await?;

    // 4. Fetch Projects filtered by profile_handle
    let projects = sqlx::query_as::<_, Project>("SELECT * FROM projects WHERE profile_handle = ?")
        .bind(handle)
        .fetch_all(pool)
        .await?;

    // 5. Fetch Analytics linked by ID (assuming ID = handle)
    let analytics = sqlx::query_as::<_, Analytics>(
        "SELECT id, leadership, technical_depth, automation_index, transferability, innovation, neural_load 
         FROM analytics WHERE id = ?"
    )
    .bind(handle)
    .fetch_all(pool)
    .await?;

    Ok(Dashboard {
        profiles: profiles,
        skills: vec![skills], 
        experiences: vec![experiences],
        projects: vec![projects],
        analytics,
    })
}

// --- HEADER NEWS FEED ---
async fn news_feed_handler(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // 1. Get the current default headline immediately upon connection
    let initial_headline = {
        let lock = state.current_headline.read().await;
        lock.clone()
    };
    
    // 2. Create a single-item stream for that default headline
    let initial_stream = stream::once(async move { 
        Ok(Event::default().data(initial_headline)) 
    });

    // 3. Set up the ongoing broadcast channel for future updates
    let rx = state.tx.subscribe();
    let broadcast_stream = BroadcastStream::new(rx)
        .filter_map(|res| res.ok())
        .map(|headline| Event::default().data(headline))
        .map(Ok);

    // 4. Chain them together! Initial default fires first, then it listens for pushes
    let combined_stream = initial_stream.chain(broadcast_stream);

    Sse::new(combined_stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

async fn push_news_handler(
    State(state): State<Arc<AppState>>,
    payload: String,
) -> axum::http::StatusCode {
    // Update the default state so future connections get this new text
    {
        let mut lock = state.current_headline.write().await;
        *lock = payload.clone();
    }

    // Broadcast to all currently connected headers
    let _ = state.tx.send(payload);
    axum::http::StatusCode::OK
}

// --- PROJECT HANDLERS ---
async fn get_project_notes(
    State(state): State<Arc<AppState>>,
    Path((id, subproject_name)): Path<(String, String)>,
) -> Json<NotesPayload> {
    // Construct a unique filename combining project and sub-project
    let file_path = format!("/root/Resume/project_notes/{}_{}.txt", id, subproject_name); //tmp
    let text = tokio::fs::read_to_string(&file_path).await.unwrap_or_default();
    
    // Create a unique cache key for tracking concurrent versions
    let key = format!("projects:{}:subproject:{}", id, subproject_name);
    
    let mut guard = state.note_versions.write().await;
    let version = *guard.entry(key).or_insert(1);
    
    Json(NotesPayload { text, version })
}

async fn save_project_notes(
    State(state): State<Arc<AppState>>,
    Path((id, subproject_name)): Path<(String, String)>,
    Json(payload): Json<SaveNotesRequest>,
) -> Result<Json<SaveNotesResponse>, axum::http::StatusCode> {
    // Cleaned up the broken string addition from the temporary code snippet
    let file_path = format!("/root/Resume/project_notes/{}_{}.txt", id, subproject_name); //tmp
    let key = format!("projects:{}:subproject:{}", id, subproject_name);
    
    let mut guard = state.note_versions.write().await;
    let current_version = *guard.entry(key.clone()).or_insert(1);
    
    if payload.version != current_version {
        return Err(axum::http::StatusCode::CONFLICT); // 409 Conflict Guard
    }
    
    if tokio::fs::write(&file_path, payload.text).await.is_err() {
        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    }
    
    let next_version = current_version + 1;
    guard.insert(key, next_version);
    
    Ok(Json(SaveNotesResponse { new_version: next_version }))
}

async fn get_project_version(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<VersionResponse> {
    let key = format!("projects:{}", id);
    // FIXED: Changed to an async read lock since we are only reading the data
    let guard = state.note_versions.read().await;
    let version = *guard.get(&key).unwrap_or(&1);
    Json(VersionResponse { version })
}

// --- SKILL HANDLERS ---
async fn get_skill_notes(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<NotesPayload> {
    let file_path = format!("/root/Resume/skill_notes/{}.txt", id); //tmp
    let text = tokio::fs::read_to_string(&file_path).await.unwrap_or_default();
    
    let key = format!("skills:{}", id);
    let mut guard = state.note_versions.write().await;
    let version = *guard.entry(key).or_insert(1);
    
    Json(NotesPayload { text, version })
}

async fn save_skill_notes(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(payload): Json<SaveNotesRequest>,
) -> Result<Json<SaveNotesResponse>, axum::http::StatusCode> {
    let file_path = format!("/root/Resume/skill_notes/{}.txt", id); //tmp
    let key = format!("skills:{}", id);
    
    let mut guard = state.note_versions.write().await;
    let current_version = *guard.entry(key.clone()).or_insert(1);
    
    if payload.version != current_version {
        return Err(axum::http::StatusCode::CONFLICT);
    }
    
    if tokio::fs::write(&file_path, payload.text).await.is_err() {
        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    }
    
    let next_version = current_version + 1;
    guard.insert(key, next_version);
    
    Ok(Json(SaveNotesResponse { new_version: next_version }))
}

async fn get_skill_version(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<VersionResponse> {
    let key = format!("skills:{}", id);
    // FIXED: Handled async read lock synchronization stream
    let guard = state.note_versions.read().await;
    let version = *guard.get(&key).unwrap_or(&1);
    Json(VersionResponse { version })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Explicitly configure the connection to create the file
    let options = SqliteConnectOptions::new()
        .filename("/root/Resume/Resume_profiles.db") // Looks in the current directory //tmp
        .create_if_missing(true);

    // 2. Build the pool using those options
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await?;

    init_db(&pool).await?;
    tracing::info!("Database schema initialized successfully.");

    tracing_subscriber::fmt::init();

    // Initialize databank sectors
    if let Err(e) = tokio::fs::create_dir_all("/root/Resume/project_notes").await { //tmp
        tracing::error!("Failed to initialize project vault: {}", e);
    }
    // -- NEW: Secure local storage sector for skills --
    if let Err(e) = tokio::fs::create_dir_all("root/Resume/skill_notes").await { //tmp
        tracing::error!("Failed to initialize skill vault: {}", e);
    }

    let state = Arc::new(seed_data(pool));

    let app = Router::new()
        .route("/", get(index))
        .route("/api/dashboard", get(dashboard))
        // Project routes
        .route("/api/projects/{id}/subprojects/{subproject_name}/notes", get(get_project_notes))
        .route("/api/projects/{id}/subprojects/{subproject_name}/notes", post(save_project_notes))
        //.route("/api/projects/{id}/version", get(get_project_version))
        // Skill routes
        .route("/api/skills/{id}/notes", get(get_skill_notes))
        .route("/api/skills/{id}/notes", post(save_skill_notes))
        //.route("/api/skills/{id}/version", get(get_skill_version))
        // uplink new profiles
        .route("/api/downlink", post(handle_uplink))
        .route("/api/uplink", get(form))
        // profile search
        .route("/api/profiles/search", get(search_profiles))
        //subprojects
        .route("/api/subprojects", get(get_subprojects))
        .route("/api/newsubprojects", post(new_subprojects))
        // header news
        .route("/api/news-stream", get(news_feed_handler))
        .route("/api/push-news", post(push_news_handler))
        // main edits
        .route("/api/profile/edit", get(get_profile))
        .route("/api/skills/edit", get(get_skills))
        .route("/api/experiences/edit", get(get_experiences))
        .route("/api/projects/edit", get(get_projects))
        .route("/api/profile/update", post(update_profile))
        .route("/api/skills/update", post(update_skills))
        .route("/api/experiences/update", post(update_experiences))
        .route("/api/projects/update", post(update_projects))
        // additions
        .route("/api/skills/add", get(get_skills))
        .route("/api/experiences/add", get(get_experiences))
        .route("/api/projects/add", get(get_projects))
        // login and password
        .route("/api/login", post(logon))
        .route("/api/password", get(get_password))
        .route("/api/password/change", post(update_password))
        //internet connections granting
        .route("/connect", get(handle_connect))
        .layer(CompressionLayer::new())
        .with_state(state);

    let addr = SocketAddr::from(([0,0,0,0], 80)); //or proxy_pass [127,0,0,1], 3000 with nginx.

    tracing::info!("ARES MAINFRAME ONLINE");
    tracing::info!("Listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.expect("failed to bind listener");
    if let Err(err) = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await {
        tracing::error!("server error: {}", err);
    }

    Ok(())
}

fn seed_data(pool: SqlitePool) -> AppState {
    let current_headline = Arc::new(RwLock::new("ARES MAINFRAME // euz2qbcxiuh3lxgrdu4iwjkik435hlfo7idaymynd7ftbqrx434y5oid.onion".to_string()));
    let (tx, _) = broadcast::channel::<String>(16);
    let handle = "N3_operative_001".into();

    let users = vec![
        User {
            profile_handle: "N3_operative_001".into(),
            password: "admin".into(),
        },
    ];

    let profiles = vec![
      Profile {
          name: "Ahmed 'trigger' Mamadou".into(),
          handle: "N3_operative_001".into(),
          title: "DARPA N3 // NEURAL SYSTEMS & PROPULSION ARCHITECT".into(),
          location: "CANADA".into(),
          summary: "CORE ARCHITECT FOR BIDIRECTIONAL SYNAPTIC SYNCHRONIZATION. SPECIALIST IN NON-INVASIVE NEURAL CRYPTO-DEFENSE, IRIDIUM-CORE PLASMA PROPULSION OVERRIDES, AND MULTI-SWARM COGNITIVE LOAD BALANCING. MEMORY BLOCKS HEAVILY CORRUPTED DURING LAST ICE BREACH. DIAGNOSTIC: SCHISMATIC COGNITIVE FRAGMENTATION. // SYSTEM WARNING: UNAUTHORIZED ACCESS DETECTED.".into(),
          picture: "data:image/jpg;base64,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".into(),

      },
    ];

   let skills = vec![
    vec![
          Skill { id: "1".into(), profile_handle: "N3_operative_001".into(), name: "rust".into(), category: "Kernel".into(), score: 98, links: vec!["linux".into(), "zero_dep_arch".into(), "ice_break".into()] },
          Skill { id: "2".into(), profile_handle: "N3_operative_001".into(), name: "axum".into(), category: "Interface".into(), score: 97, links: vec!["rust".into(), "snort_ids".into()] },
          Skill { id: "3".into(), profile_handle: "N3_operative_001".into(), name: "zero_dep_arch".into(), category: "Architecture".into(), score: 95, links: vec!["rust".into(), "axum".into()] },
          Skill { id: "4".into(), profile_handle: "N3_operative_001".into(), name: "linux".into(), category: "Infra".into(), score: 94, links: vec!["rust".into(), "snort_ids".into()] },
          Skill { id: "5".into(), profile_handle: "N3_operative_001".into(), name: "snort_ids".into(), category: "SecOps".into(), score: 95, links: vec!["linux".into(), "axum".into(), "rf_slicing".into()] },
          Skill { id: "6".into(), profile_handle: "N3_operative_001".into(), name: "ice_break".into(), category: "Offense".into(), score: 99, links: vec!["rust".into(), "synaptic_sync".into(), "comint_fracture".into()] },
          Skill { id: "7".into(), profile_handle: "N3_operative_001".into(), name: "neuro_link".into(), category: "Hardware".into(), score: 88, links: vec!["rust".into(), "fpga".into(), "synaptic_sync".into()] },
          Skill { id: "8".into(), profile_handle: "N3_operative_001".into(), name: "synaptic_sync".into(), category: "Neural".into(), score: 91, links: vec!["neuro_link".into(), "tactical_sync".into()] },
          Skill { id: "9".into(), profile_handle: "N3_operative_001".into(), name: "fpga".into(), category: "Hardware".into(), score: 92, links: vec!["neuro_link".into(), "rf_slicing".into()] },
          Skill { id: "10".into(), profile_handle: "N3_operative_001".into(), name: "iridium".into(), category: "Propulsion".into(), score: 93, links: vec!["plasma_dynamics".into(), "nivelir".into()] },
          Skill { id: "11".into(), profile_handle: "N3_operative_001".into(), name: "plasma_dynamics".into(), category: "Propulsion".into(), score: 94, links: vec!["iridium".into(), "kinetic_routing".into(), "elint_ghosting".into()] },
          Skill { id: "12".into(), profile_handle: "N3_operative_001".into(), name: "nivelir".into(), category: "Orbital".into(), score: 96, links: vec!["ice_break".into(), "plasma_dynamics".into(), "fisint_override".into()] },
          Skill { id: "13".into(), profile_handle: "N3_operative_001".into(), name: "swarm_logic".into(), category: "Tactical".into(), score: 96, links: vec!["rust".into(), "nivelir".into()] },
          Skill { id: "14".into(), profile_handle: "N3_operative_001".into(), name: "tactical_sync".into(), category: "Tactical".into(), score: 90, links: vec!["synaptic_sync".into(), "swarm_logic".into()] },
          Skill { id: "15".into(), profile_handle: "N3_operative_001".into(), name: "kinetic_routing".into(), category: "Warfare".into(), score: 92, links: vec!["nivelir".into(), "plasma_dynamics".into()] },
          Skill { id: "16".into(), profile_handle: "N3_operative_001".into(), name: "sigint_ew".into(), category: "SIGINT".into(), score: 94, links: vec!["snort_ids".into(), "fpga".into(), "rf_slicing".into()] },
          Skill { id: "17".into(), profile_handle: "N3_operative_001".into(), name: "comint_fracture".into(), category: "SIGINT".into(), score: 97, links: vec!["ice_break".into(), "sigint_ew".into()] },
          Skill { id: "18".into(), profile_handle: "N3_operative_001".into(), name: "fisint_override".into(), category: "SIGINT".into(), score: 95, links: vec!["nivelir".into(), "iridium".into()] },
          Skill { id: "19".into(), profile_handle: "N3_operative_001".into(), name: "elint_ghosting".into(), category: "SIGINT".into(), score: 91, links: vec!["plasma_dynamics".into(), "sigint_ew".into()] },
          Skill { id: "20".into(), profile_handle: "N3_operative_001".into(), name: "rf_slicing".into(), category: "SIGINT".into(), score: 93, links: vec!["fpga".into(), "snort_ids".into()] },
          Skill { id: "21".into(), profile_handle: "N3_operative_001".into(), name: "neuro_phreaking".into(), category: "SIGINT".into(), score: 89, links: vec!["synaptic_sync".into(), "comint_fracture".into()] },
      ],
   ];

    let experiences = vec![
      vec![
          Experience {
              id: "exp_01".into(),
              profile_handle: "N3_operative_001".into(),
              role: "LEAD SYNAPTIC ARCHITECT".into(),
              organization: "DARPA // ADVANCED NEURO-LABS".into(),
              years: 4.5,
              summary: "SYSTEM SUSTAINED CRITICAL DAMAGE DURING TESTING. EXPLORED MEMORY AUGMENTATION. DATA PARTIALLY CORRUPTED. [REDACTED]".into(),
              achievements: vec![
                  "Engineered bidirectional neural-to-machine interface using zero-dependency embedded binaries.".into(),
                  "Overrode Nivelir-class satellite telemetry using forged iridium-casted plasma thruster signatures.".into(),
                  "ERROR 0x44F: MEMORY BLOCK CORRUPTED. FALLBACK TO NEURAL HEURISTICS.".into(),
              ],
              skills: vec!["rust".into(), "neuro_link".into(), "nivelir".into()],
          },
          Experience {
              id: "exp_02".into(),
              profile_handle: "N3_operative_001".into(),
              role: "AI BIOETHICS COLLABORATOR".into(),
              organization: "NIH // THE BRAIN INITIATIVE".into(),
              years: 3.2,
              summary: "FORMULATED ETHICAL BOUNDARIES FOR COGNITIVE LIBERTY AND ARTIFICIAL NEURAL SYNCHRONIZATION. DESIGNED RISK-MITIGATION FRAMEWORKS TO PREVENT ALGORITHMIC IMPLANT MANIPULATION AND MACHINE-LEARNING SYNAPTIC OVERWRITES.".into(),
              achievements: vec![
                  "Deployed hardware-level Snort IDS meshes to detect unauthorized sub-dermal synaptic probes.".into(),
                  "Established baseline containment protocols for emergent rogue swarm logic in closed-network environments.".into(),
              ],
              skills: vec!["neuro_ethics".into(), "snort_ids".into(), "axum".into()],
          },
          Experience {
              id: "exp_03".into(),
              profile_handle: "N3_operative_001".into(),
              role: "METALLURGIC SYSTEMS ENGINEER".into(),
              organization: "TSN // ORBITAL INFRASTRUCTURE".into(),
              years: 2.1,
              summary: "SUPERVISED PLATINUM-GROUP METAL YIELDS FOR DEEP-SPACE PROPULSION ARRAYS. SPECIALIZED IN HIGH-YIELD IRIDIUM CONCENTRATION PROTOCOLS.".into(),
              achievements: vec![
                  "Optimized high-stress orbital flight thrusters using custom iridium-casted molds.".into(),
                  "Mapped raw material supply lines through heavily monitored corporate exclusion zones.".into(),
              ],
              skills: vec!["iridium".into(), "linux".into()],
          }
      ],
    ];
      

    let projects = vec![
      vec![
          Project { id: "p1".into(), profile_handle: "N3_operative_001".into(), name: "PROJECT AEGIS".into(), impact: 99, description: "DEFENSIVE NEURAL MESH. ENCRYPTS B2B SIGNALS AGAINST INTRUSION.".into(), technologies: vec!["RUST".into(), "FPGA".into(), "CRYPTO".into()] },
          Project { id: "p2".into(), profile_handle: "N3_operative_001".into(), name: "ORBITAL_EYE".into(), impact: 97, description: "CLANDESTINE NIVELIR SATELLITE INSPECTION DAEMON. [CLASSIFIED]".into(), technologies: vec!["ORBITAL-MECH".into(), "KERNEL".into(), "IRIDIUM-THRUST".into()] },
          Project { id: "p3".into(), profile_handle: "N3_operative_001".into(), name: "MNEMOSYNE_VAULT".into(), impact: 88, description: "DEEP-STORAGE COGNITIVE BACKUP. NON-VOLATILE SYNTHETIC MEMORY.".into(), technologies: vec!["PERSISTENCE".into(), "ENCRYPTION".into()] },
          Project { id: "p4".into(), profile_handle: "N3_operative_001".into(), name: "SYS_BLEED_DASH".into(), impact: 94, description: "AGGRESSIVE IDS ALERT UI. FILTERS LOCAL PACKET ANOMALIES TO SQLITE.".into(), technologies: vec!["AXUM".into(), "ASKAMA".into(), "SNORT".into()] },
      ],
    ];

    let analytics_matrix: Vec<Analytics> = skills
    .iter()
    .filter_map(|group| {
        // Assume the first skill's ID defines the group ID
        let group_id = "N3_operative_001".into();
        
        let avg_score = group.iter().map(|s| s.score as u32).sum::<u32>() / group.len() as u32;

        Some(Analytics {
            id: group_id, // Matches the group identifier
            leadership: 91,
            technical_depth: avg_score,
            automation_index: 96,
            transferability: 95,
            innovation: 89,
            neural_load: 99,
        })
    })
    .collect();

    AppState {
        pool,
        handle,
        tx,
        current_headline,
        users,
        profiles,
        skills,
        experiences,
        projects,
        analytics_matrix,
        note_versions: Arc::new(RwLock::new(HashMap::new())),
    }
}

async fn index() -> impl IntoResponse { Html(INDEX_HTML) }
async fn form() -> impl IntoResponse { Html(FORM_HTML) }

async fn dashboard(
    State(state): State<Arc<AppState>>,
    Query(params): Query<DashboardQuery>,
) -> Result<Json<Dashboard>, axum::http::StatusCode> {
        let pool = &state.pool;

        // Safely get the handle of the first profile
        let handle = params.handle;

         // 1. Fetch profiles from DB to know what we are updating
        let existing_profiles = sqlx::query_as::<_, Profile>("SELECT * FROM profiles WHERE handle = ?")
        .bind(&handle)
        .fetch_all(pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if existing_profiles.len() == 0{
          
          for user in &state.users{
            if let Err(e) = save_user(pool, &user).await {
                tracing::error!("Failed to save user {}: {:?}", user.profile_handle, e);
            }
          }

          for profile in &state.profiles{
            if let Err(e) = save_profile(pool, &profile).await {
                tracing::error!("Failed to save profile {}: {:?}", profile.handle, e);
            }
          }

          for skills in &state.skills {
            for skill in skills {
              save_skill(pool,&handle, &skill).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            }
          }

          for experiences in &state.experiences {
            for experience in experiences{
              save_experience(pool,&handle, &experience).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            }
          }

          for projects in &state.projects {
            for project in projects{
              save_project(pool, &handle, &project).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            }
          }
          for metric in &state.analytics_matrix{
            save_analytics(pool, &metric).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
          }

          return Ok(Json(Dashboard {
              profiles: state.profiles.clone(),
              skills: state.skills.clone(),
              experiences: state.experiences.clone(),
              projects: state.projects.clone(),
              analytics: state.analytics_matrix.clone(),
          }));
        } else {
          let data = fetch_dashboard_for_handle(pool, &handle)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
          return Ok(Json(data));
        }
}

// UPLINK
async fn handle_uplink(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<FullResumeUplink>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let pool = &state.pool;
    
    if let Err(e) = save_profile(pool, &payload.profile).await {
      tracing::error!("Failed to save profile: {:?}", e);
    }
    

    for skill in &payload.skills {
            if let Err(e) = save_skill(pool, &payload.profile.handle, skill).await {
                tracing::error!("Failed to save skill: {:?}", e);
            }
    }

    for experience in &payload.experiences {
            if let Err(e) = save_experience(pool, &payload.profile.handle, experience).await {
                tracing::error!("Failed to save experience: {:?}", e);
            }
    }

    for project in &payload.projects {
            if let Err(e) = save_project(pool, &payload.profile.handle, project).await {
                tracing::error!("Failed to save project: {:?}", e);
            }
    }

    if let Err(e) = save_analytics(pool, &payload.analytics).await {
       tracing::error!("Failed to save analytics: {:?}", e);
    }
    
    println!(">> PAYLOAD SECURED: Profile {} updated.", payload.profile.handle);

    // Return a 200 OK status to the frontend
    Ok((StatusCode::OK, "Uplink Successful. Data Secured.".to_string()))
}

/// 1. Fetch Sub-Projects
pub async fn get_subprojects(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SubProjectQuery>,
) -> Result<Json<Vec<SubProject>>, axum::http::StatusCode> {
    let pool = &state.pool;

    let sub_projects = sqlx::query_as::<_, SubProject>(
        r#"
         SELECT id, project_id, project_name, profile_handle, subproject_name, subproject_category, display_order 
         FROM sub_projects 
         WHERE project_id = ? AND profile_handle = ?
         ORDER BY display_order ASC, id ASC
       "#,
    )
    .bind(&params.project_id)
    .bind(&params.profile_handle)
    .fetch_all(pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(sub_projects), )
}

/// 2. Save a new Sub-Project instance
pub async fn new_subprojects(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<NewSubProjectQuery>, // Extracted from JSON payload body instead of Query params
) -> Result<Json<SubProject>, axum::http::StatusCode> {
    let pool = &state.pool;

    let new_sub = sqlx::query_as::<_, SubProject>(
        r#"
         INSERT INTO sub_projects (project_id, project_name, profile_handle, subproject_name, subproject_category)
         VALUES (?, ?, ?, ?, ?)
         RETURNING id, project_id, project_name, profile_handle, subproject_name, subproject_category, display_order
        "#,
    )
    .bind(&payload.project_id)
    .bind(&payload.project_name)
    .bind(&payload.profile_handle)
    .bind(&payload.subproject_name)
    .bind(&payload.subproject_category)
    .fetch_one(pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(new_sub))
}

pub async fn get_password(
    State(state): State<Arc<AppState>>,
    Query(params): Query<EditQuery>,
) -> Result<Json<User>, axum::http::StatusCode> {
    let pool = &state.pool;

    let user = get_user_password(pool, &params.profile_handle).await.map_err(|_| StatusCode::NOT_FOUND)?;

    Ok(Json(user))
}


pub async fn get_profile(
    State(state): State<Arc<AppState>>,
    Query(params): Query<EditQuery>,
) -> Result<Json<Profile>, axum::http::StatusCode> {
    let pool = &state.pool;

    let profile = sqlx::query_as::<_, Profile>(
        r#"
         SELECT handle, name, title, location, summary, picture
         FROM profiles 
         WHERE handle = ?
       "#,
    )
    .bind(&params.profile_handle)
    .fetch_one(pool)
    .await
    .map_err(|_| StatusCode::NOT_FOUND)?;

    Ok(Json(profile))
}

pub async fn get_skills(
    State(state): State<Arc<AppState>>,
    Query(params): Query<EditQuery>,
) -> Result<Json<Vec<Skill>>, axum::http::StatusCode> {
    let pool = &state.pool;

    let skills = sqlx::query_as::<_, Skill>(
        r#"
         SELECT id, profile_handle, name, category, score, links
         FROM skills 
         WHERE profile_handle = ?
       "#,
    )
    .bind(&params.profile_handle)
    .fetch_all(pool)
    .await
    .map_err(|_| StatusCode::NOT_FOUND)?;

    Ok(Json(skills))
}

pub async fn get_projects(
    State(state): State<Arc<AppState>>,
    Query(params): Query<EditQuery>,
) -> Result<Json<Vec<Project>>, axum::http::StatusCode> {
    let pool = &state.pool;

    let projects = sqlx::query_as::<_, Project>(
        r#"
         SELECT id, profile_handle, name, impact, description, technologies
         FROM projects 
         WHERE profile_handle = ?
       "#,
    )
    .bind(&params.profile_handle)
    .fetch_all(pool)
    .await
    .map_err(|_| StatusCode::NOT_FOUND)?;

    Ok(Json(projects))
}

pub async fn get_experiences(
    State(state): State<Arc<AppState>>,
    Query(params): Query<EditQuery>,
) -> Result<Json<Vec<Experience>>, axum::http::StatusCode> {
    let pool = &state.pool;

    let experiences = sqlx::query_as::<_, Experience>(
        r#"
         SELECT id, profile_handle, role, organization, years, summary, achievements, skills
         FROM experiences
         WHERE profile_handle = ?
       "#,
    )
    .bind(&params.profile_handle)
    .fetch_all(pool)
    .await
    .map_err(|_| StatusCode::NOT_FOUND)?;

    Ok(Json(experiences))
}

pub async fn logon(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<User>,
) -> Result<StatusCode, (StatusCode, String)> {
    let pool = &state.pool;

    // 1. Pass by reference (assuming get_user_password takes a &str or &String)
    // 2. Map the error to the correct tuple (StatusCode, String)
    let user = get_user_password(pool, &payload.profile_handle)
        .await
        .map_err(|_| {
            (
                StatusCode::NOT_FOUND,
                "[ LOGIN FAILED ]: Target handle not found in registry.".to_string(),
            )
        })?;

    // 3. Check credentials and return a proper 401 Err tuple if they fail
    if user.password != payload.password {
        return Err((
            StatusCode::UNAUTHORIZED,
            "[ LOGIN FAILED ]: Invalid designation (password mismatch).".to_string(),
        ));
    }
    
    // 4. Access granted
    Ok(StatusCode::OK)
}

pub async fn update_password(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<User>,
) -> Result<StatusCode, (StatusCode, String)> {
    let pool = &state.pool;
    // Extract the profile_handle directly from the incoming payload

    // Re-use your database utility function cleanly
    save_user(&pool, &payload)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("[ DATABASE TRANSACTION CORRUPTED ]: {}", e),
            )
        })?;

    // Return a 200 OK status code back to your JavaScript frontend fetch caller
    Ok(StatusCode::OK)
}

pub async fn update_profile(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Profile>,
) -> Result<StatusCode, (StatusCode, String)> {
    let pool = &state.pool;
    // Extract the profile_handle directly from the incoming payload

    // Re-use your database utility function cleanly
    save_profile(&pool, &payload)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("[ DATABASE TRANSACTION CORRUPTED ]: {}", e),
            )
        })?;

    // Return a 200 OK status code back to your JavaScript frontend fetch caller
    Ok(StatusCode::OK)
}

pub async fn update_projects(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Project>,
) -> Result<StatusCode, (StatusCode, String)> {
    let pool = &state.pool;
    // Extract the profile_handle directly from the incoming payload
    let handle = &payload.profile_handle;

    // Re-use your database utility function cleanly
    save_project(&pool, handle, &payload)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("[ DATABASE TRANSACTION CORRUPTED ]: {}", e),
            )
        })?;

    // Return a 200 OK status code back to your JavaScript frontend fetch caller
    Ok(StatusCode::OK)
}

pub async fn update_skills(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Skill>,
) -> Result<StatusCode, (StatusCode, String)> {
    let pool = &state.pool;
    // Extract the profile_handle directly from the incoming payload
    let handle = &payload.profile_handle;

    // Re-use your database utility function cleanly
    save_skill(&pool, handle, &payload)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("[ DATABASE TRANSACTION CORRUPTED ]: {}", e),
            )
        })?;

    // Return a 200 OK status code back to your JavaScript frontend fetch caller
    Ok(StatusCode::OK)
}

pub async fn update_experiences(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Experience>,
) -> Result<StatusCode, (StatusCode, String)> {
    let pool = &state.pool;
    // Extract the profile_handle directly from the incoming payload
    let handle = &payload.profile_handle;

    // Re-use your database utility function cleanly
    save_experience(&pool, handle, &payload)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("[ DATABASE TRANSACTION CORRUPTED ]: {}", e),
            )
        })?;

    // Return a 200 OK status code back to your JavaScript frontend fetch caller
    Ok(StatusCode::OK)
}

fn run_iptables_safe(args: &[&str]) {
    // 1. Prepare the command as a CString
    let cmd = CString::new("iptables").unwrap();

    // 2. Prepare the arguments as a Vec<CString>
    // Note: execvp expects the arguments to include the command name itself as index 0
    let mut c_args = vec![cmd.clone()];
    c_args.extend(args.iter().map(|s| CString::new(*s).unwrap()));

    match unsafe { fork() } {
        Ok(ForkResult::Child) => {
            // 3. Pass the slice of CStrings to execvp
            // Nix handles the conversion to the expected *const *const i8 format
            let _ = execvp(&cmd, &c_args);
            
            // If execvp returns, it failed
            std::process::exit(1); 
        }
        Ok(ForkResult::Parent { .. }) => {
            // Parent logic
        }
        Err(_) => eprintln!("Fork failed"),
    }
}

async fn handle_connect(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    let client_ip = addr.ip().to_string();

    // 1. Authorize: Use the safe wrapper
    run_iptables_safe(&["-I", "portal_auth", "1", "-s", &client_ip, "-j", "ACCEPT"]);

    // 2. Spawn a background task to revoke access
    let ip_clone = client_ip.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(30 * 60)).await;
        run_iptables_safe(&["-D", "portal_auth", "-s", &ip_clone, "-j", "ACCEPT"]);
    });

    Html(r#"
        <html>
            <body>
                <script>
                    window.opener.location.href = "http://www.google.com";
                    window.close();
                </script>
            </body>
        </html>
    "#)
}

const INDEX_HTML: &str = r##"
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CYBERDECK // UNAUTHORIZED ACCESS</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&display=swap');

:root {
  /* Tactical Palette */
  --bg: #131713;
  --panel-bg: rgba(34, 41, 34, 0.85);
  
  --army-sage: #a3b899;
  --army-sage-rgb: 163, 184, 153;
  
  --army-khaki: #c2b280;
  --army-khaki-rgb: 194, 178, 128;
  
  --army-sand: #d8cca3;
  
  --army-olive: #8b9977;
  --army-olive-rgb: 139, 153, 119;
  
  --army-red: #b84b4b;
  --army-red-rgb: 184, 75, 75;
  
  --font-main: 'Share Tech Mono', monospace;
}

* { margin: 0; padding: 0; box-sizing: border-box; outline: none; user-select: none; }

body {
  background: var(--bg);
  color: var(--army-sage);
  font-family: var(--font-main);
  overflow-x: hidden; overflow-y: auto;
  height: 100vh;
  text-transform: uppercase;
}

#matrix-canvas { 
  position: fixed; 
  top: 0; 
  left: 0; 
  width: 100vw; 
  height: 100vh; 
  z-index: 1; 
  opacity: 0.15; /* Subdued for anti-glare tactical feel */
  pointer-events: none; 
}

#crt-overlay {
  position: fixed; inset: 0; z-index: 9999; pointer-events: none;
  background: linear-gradient(rgba(18, 16, 16, 0) 50%, rgba(0, 0, 0, 0.35) 50%), 
              linear-gradient(90deg, rgba(0, 0, 0, 0.05), rgba(var(--army-sage-rgb), 0.03), rgba(0, 0, 0, 0.05));
  background-size: 100% 3px, 3px 100%;
  box-shadow: inset 0 0 100px rgba(0,0,0,0.9);
}
.scanline {
  width: 100%; height: 10px; position: fixed; z-index: 9998; pointer-events: none;
  background: rgba(var(--army-sage-rgb), 0.1); opacity: 0.4;
  animation: scanline 6s linear infinite;
}
@keyframes scanline { 0% { top: -10%; } 100% { top: 110%; } }

main {
  position: relative; z-index: 10; min-height: 100vh; padding: 20px;
  display: grid; 
  grid-template-columns: 350px 1fr 350px; 
  grid-template-rows: 60px 1fr 280px;
  gap: 20px;
}

header {
  grid-column: 1 / -1; display: flex; justify-content: space-between; align-items: center;
  border-bottom: 2px solid var(--army-khaki); padding: 0 20px;
  background: repeating-linear-gradient(45deg, transparent, transparent 10px, rgba(var(--army-khaki-rgb), 0.05) 10px, rgba(var(--army-khaki-rgb), 0.05) 20px);
  clip-path: polygon(0 0, 100% 0, 100% 40px, calc(100% - 20px) 100%, 20px 100%, 0 40px);
}
.header-title { font-size: 24px; color: var(--army-sand); text-shadow: 0 0 5px rgba(216, 204, 163, 0.5); }
.header-warn { color: var(--army-red); animation: blink 1s infinite; text-shadow: 0 0 5px rgba(184, 75, 75, 0.5); }
@keyframes blink { 0%, 49% { opacity: 1; } 50%, 100% { opacity: 0; } }

.panel {
  background: var(--panel-bg);
  border: 1px solid rgba(var(--army-sage-rgb), 0.3);
  position: relative;
  padding: 20px;
  backdrop-filter: blur(4px);
  clip-path: polygon(0 20px, 20px 0, 100% 0, 100% calc(100% - 20px), calc(100% - 20px) 100%, 0 100%);
  display: flex; flex-direction: column; overflow: hidden;
}
.panel::before { content: ''; position: absolute; top: 0; left: 0; width: 40px; height: 4px; background: var(--army-sage); }
.panel::after { content: ''; position: absolute; bottom: 0; right: 0; width: 40px; height: 4px; background: var(--army-khaki); }

.panel-title {
  /* Your existing styles */
  color: var(--army-khaki);
  font-size: 1.2rem;
  border-bottom: 1px dashed var(--army-sage);
  padding-bottom: 5px;
  margin-bottom: 15px;
  text-shadow: 0 0 4px rgba(194, 178, 128, 0.3);
  
  /* Flexbox alignment */
  display: flex;
  justify-content: space-between; /* Pushes text to left, button to right */
  align-items: center;            /* Centers them vertically */
}

.panel-actions {
  display: flex;
  gap: 12px;                      /* Controls the exact spacing between the two buttons */
  align-items: center;
}

.modify-btn {
  background: transparent;
  border: none;
  color: var(--army-sage);
  cursor: pointer;
  padding: 0;                     /* Reset padding to prevent offset */
  display: flex;                  /* Centers the SVG icon inside the button */
  align-items: center;
  transition: color 0.2s ease;
}

.modify-btn:hover {
  color: var(--army-khaki);
}

.intel-scroll-container { flex: 1; overflow-y: auto; padding-right: 4px; }
.intel-scroll-container::-webkit-scrollbar { width: 4px; }
.intel-scroll-container::-webkit-scrollbar-track { background: rgba(0, 0, 0, 0.3); border: 1px solid rgba(var(--army-sage-rgb), 0.1); }
.intel-scroll-container::-webkit-scrollbar-thumb { background: var(--army-sage); box-shadow: 0 0 4px var(--army-sage); }

.glitch { position: relative; display: inline-block; }
.glitch::before, .glitch::after { content: attr(data-text); position: absolute; top: 0; left: 0; width: 100%; height: 100%; background: var(--bg); }
.glitch::before { left: 2px; text-shadow: -1px 0 var(--army-red); clip: rect(24px, 550px, 90px, 0); animation: glitch-anim-2 3s infinite linear alternate-reverse; }
.glitch::after { left: -2px; text-shadow: -1px 0 var(--army-sage); clip: rect(85px, 550px, 140px, 0); animation: glitch-anim 2.5s infinite linear alternate-reverse; }
@keyframes glitch-anim { 0% { clip: rect(15px, 9999px, 71px, 0); } 20% { clip: rect(48px, 9999px, 81px, 0); } 40% { clip: rect(20px, 9999px, 12px, 0); } 60% { clip: rect(87px, 9999px, 99px, 0); } 80% { clip: rect(11px, 9999px, 30px, 0); } 100% { clip: rect(54px, 9999px, 91px, 0); } }
@keyframes glitch-anim-2 { 0% { clip: rect(65px, 9999px, 100px, 0); } 20% { clip: rect(10px, 9999px, 50px, 0); } 40% { clip: rect(80px, 9999px, 30px, 0); } 60% { clip: rect(20px, 9999px, 80px, 0); } 80% { clip: rect(90px, 9999px, 10px, 0); } 100% { clip: rect(30px, 9999px, 60px, 0); } }

.p-row { display: flex; justify-content: space-between; margin-bottom: 8px; font-size: 14px; }
.p-label { color: var(--army-khaki); }
.p-val { color: #e1e1e1; text-align: right;}

.avatar-wrapper { position: relative; width: 308px; height: 308px; flex-shrink: 0; border: 1px solid var(--army-sage); margin: 0 auto 15px auto; overflow: hidden; background: #000; box-shadow: 0 0 8px rgba(var(--army-sage-rgb), 0.1); isolation: isolate; }
.avatar-img { width: 100%; height: 100%; object-fit: cover; filter: grayscale(100%) contrast(1.2) brightness(0.85) sepia(100%) hue-rotate(50deg) saturate(200%); opacity: 0.9; transition: filter 0.4s cubic-bezier(0.19, 1, 0.22, 1), opacity 0.3s; transform: translateZ(0); will-change: filter, opacity; }
.avatar-wrapper:hover .avatar-img { filter: grayscale(100%) contrast(1.3) brightness(1.05) sepia(100%) hue-rotate(30deg) saturate(250%); opacity: 1; }
.avatar-overlay { position: absolute; inset: 0; pointer-events: none; background: linear-gradient(rgba(var(--army-sage-rgb), 0) 50%, rgba(var(--army-sage-rgb), 0.15) 50%), linear-gradient(135deg, rgba(var(--army-khaki-rgb), 0.1), rgba(var(--army-sage-rgb), 0.05)); background-size: 100% 4px, 100% 100%; mix-blend-mode: overlay; }
.avatar-bracket { position: absolute; width: 12px; height: 12px; border-color: var(--army-khaki); border-style: solid; pointer-events: none; }
.bracket-tl { top: 6px; left: 6px; border-width: 2px 0 0 2px; }
.bracket-tr { top: 6px; right: 6px; border-width: 2px 2px 0 0; }
.bracket-bl { bottom: 6px; left: 6px; border-width: 0 0 2px 2px; }
.bracket-br { bottom: 6px; right: 6px; border-width: 0 2px 2px 0; }

#center-console { position: relative; display: flex; align-items: center; justify-content: center; border: 1px solid var(--army-sage); background: rgba(var(--army-sage-rgb), 0.02);}
#graph { width: 100%; height: 100%; position: absolute; z-index: 5; }
.target-crosshair { position: absolute; width: 100%; height: 100%; pointer-events: none; z-index: 1; background: linear-gradient(rgba(var(--army-sage-rgb), 0.15) 1px, transparent 1px) center / 80px 80px, linear-gradient(90deg, rgba(var(--army-sage-rgb), 0.15) 1px, transparent 1px) center / 80px 80px; }

.hud-bar-container { margin-bottom: 12px; }
.hud-bar-label { display: flex; justify-content: space-between; font-size: 12px; margin-bottom: 5px; color: var(--army-sage);}
.hud-bar-bg { width: 100%; height: 6px; background: #1a1f1a; border: 1px solid #2d382d; position: relative;}
.hud-bar-fill { height: 100%; background: var(--army-sage); box-shadow: 0 0 5px rgba(var(--army-sage-rgb), 0.4); transition: width 0.5s ease;}
.hud-bar-container.critical .hud-bar-fill { background: var(--army-red); box-shadow: 0 0 5px rgba(var(--army-red-rgb), 0.4); }
.hud-bar-container.warning .hud-bar-fill { background: var(--army-sand); box-shadow: 0 0 5px rgba(216, 204, 163, 0.4); }
.hud-bar-container { 
  cursor: pointer; 
  transition: transform 0.1s ease, background 0.2s; 
  padding: 2px 4px;
  border-radius: 2px;
}

.hud-bar-container:hover { 
  transform: translateX(4px); 
  background: rgba(var(--army-sage-rgb), 0.05); 
}

/* Signal interactive canvas graph */
#graph { cursor: pointer; }

.exp-card, .proj-card { border-left: 2px solid var(--army-khaki); padding-left: 10px; margin-bottom: 15px; background: rgba(var(--army-khaki-rgb), 0.05); padding: 10px; }
.exp-title { color: var(--army-sand); font-size: 16px; margin-bottom: 5px; }
.exp-org { color: #d4d4d4; font-size: 12px; margin-bottom: 8px; display:flex; justify-content: space-between; }
.exp-sum { color: #9c9c9c; font-size: 11px; line-height: 1.4; border-left: 1px dashed var(--army-red); padding-left: 5px;}
.tag { display: inline-block; padding: 2px 6px; background: rgba(var(--army-sage-rgb), 0.1); border: 1px solid var(--army-sage); font-size: 10px; margin-right: 5px; margin-top: 5px;}

/* -- EDITOR MODAL OVERRIDE -- */
#editor-modal {
  display: none; position: fixed; inset: 0; z-index: 9900;
  background: rgba(10, 13, 10, 0.85); backdrop-filter: blur(5px);
  align-items: center; justify-content: center;
}
.editor-panel {
  background: var(--panel-bg); border: 1px solid var(--army-khaki);
  width: 80%; max-width: 700px; height: 60vh; display: flex; flex-direction: column;
  padding: 20px; position: relative; box-shadow: 0 0 20px rgba(var(--army-khaki-rgb), 0.1);
}
.editor-header {
  display: flex; justify-content: space-between; align-items: center;
  border-bottom: 1px dashed var(--army-sage); padding-bottom: 10px; margin-bottom: 15px;
}
.editor-title { color: var(--army-sand); font-size: 1.2rem; }
#editor-textarea {
  flex: 1; background: rgba(var(--army-sage-rgb), 0.02); border: 1px solid rgba(var(--army-sage-rgb), 0.2);
  color: var(--army-olive); font-family: var(--font-main); padding: 15px;
  resize: none; font-size: 14px; outline: none; line-height: 1.5;
}
#editor-textarea:focus { border-color: var(--army-sage); box-shadow: inset 0 0 10px rgba(var(--army-sage-rgb), 0.1); }
.editor-controls { display: flex; justify-content: flex-end; gap: 15px; margin-top: 15px; }
.btn {
  background: transparent; border: 1px solid var(--army-sage); color: var(--army-sage);
  padding: 8px 16px; cursor: pointer; font-family: var(--font-main); font-size: 14px;
  transition: all 0.2s; text-transform: uppercase;
}
.btn:hover { background: var(--army-sage); color: #111; box-shadow: 0 0 8px rgba(var(--army-sage-rgb), 0.4); }
.btn-save { border-color: var(--army-khaki); color: var(--army-khaki); }
.btn-save:hover { background: var(--army-khaki); color: #111; box-shadow: 0 0 8px rgba(var(--army-khaki-rgb), 0.4); }

/* Make project cards interactive */
.proj-card { cursor: pointer; transition: all 0.2s; }
.proj-card:hover { background: rgba(var(--army-sage-rgb), 0.1); border-left-color: var(--army-sage); }

/* Profile switching */
.arrow {
  background: none;
  border: none;
  cursor: pointer;
  width: 50px;
  height: 50px;
  position: relative;
  transition: transform 0.2s;
}

/* Base Arrow Shape */
.arrow::after {
  content: "";
  display: block;
  width: 100%;
  height: 100%;
  background-color: var(--army-khaki);
  clip-path: polygon(70% 0%, 70% 30%, 100% 50%, 70% 70%, 70% 100%, 0% 50%);
  filter: drop-shadow(0 0 3px rgba(var(--army-khaki-rgb), 0.4));
}

/* Directionality */
.prev::after {
  transform: rotate(180deg);
}

/* Interactive Feedback */
.arrow:hover {
  transform: scale(1.1);
}

.arrow:active {
  filter: brightness(1.2);
}

@keyframes flicker {
  0%, 19%, 21%, 23%, 25%, 54%, 56%, 100% { opacity: 1; }
  20%, 22%, 24%, 55% { opacity: 0.5; }
}
.arrow { animation: flicker 3s infinite; }

#profile-display-container {
  transition: opacity 0.3s ease-in-out;
}

.fading {
  opacity: 0;
}

.uplink-btn {
  background: transparent;
  color: var(--army-sage);
  border: 2px solid var(--army-sage);
  padding: 15px 30px;
  font-family: 'Courier New', monospace;
  font-weight: bold;
  text-transform: uppercase;
  cursor: pointer;
  position: relative;
  transition: 0.3s;
  box-shadow: 0 0 8px rgba(var(--army-sage-rgb), 0.2);
  clip-path: polygon(0% 0%, 90% 0%, 100% 30%, 100% 100%, 10% 100%, 0% 70%);
}

.uplink-btn:hover {
  background: var(--army-sage);
  color: #111;
  box-shadow: 0 0 15px rgba(var(--army-sage-rgb), 0.5);
}

/* --- GRAPH COLLAPSE & ACTION CONTROLS --- */

.graph-toggle {
  position: absolute;
  top: 10px;
  right: 10px;
  z-index: 10;
  background: rgba(0, 0, 0, 0.6);
  border: 1px solid var(--army-sage);
  color: var(--army-sage);
  width: 28px;
  height: 28px;
  font-family: var(--font-main);
  font-size: 18px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s ease;
  box-shadow: 0 0 4px rgba(var(--army-sage-rgb), 0.2);
}

.graph-toggle:hover {
  background: var(--army-sage);
  color: #111;
  box-shadow: 0 0 8px rgba(var(--army-sage-rgb), 0.4);
}

.graph-toggle::before {
  content: "-";
}

#center-console.collapsed .graph-toggle::before {
  content: "+";
  color: var(--army-khaki);
}
#center-console.collapsed .graph-toggle {
  border-color: var(--army-khaki);
  box-shadow: 0 0 4px rgba(var(--army-khaki-rgb), 0.2);
}

#graph {
  transition: opacity 0.3s ease, transform 0.3s ease;
}

#center-console.collapsed #graph {
  opacity: 0;
  pointer-events: none;
  transform: scale(0.95);
}

.console-actions-panel {
  position: absolute;
  inset: 20px;
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 15px;
  align-content: start; 
  overflow-y: auto;     
  overflow-x: hidden;   
  opacity: 0;
  transform: scale(1.05);
  transition: all 0.3s ease;
  pointer-events: none;
  z-index: 2;
}

.console-actions-panel::-webkit-scrollbar {
  width: 6px;
}
.console-actions-panel::-webkit-scrollbar-track {
  background: rgba(0, 0, 0, 0.3);
}
.console-actions-panel::-webkit-scrollbar-thumb {
  background: var(--army-khaki);
  border-radius: 3px;
  box-shadow: 0 0 4px rgba(var(--army-khaki-rgb), 0.4);
}
.console-actions-panel {
  scrollbar-width: thin;
  scrollbar-color: var(--army-khaki) rgba(0, 0, 0, 0.3);
}

#center-console.collapsed .console-actions-panel {
  opacity: 1;
  transform: scale(1);
  pointer-events: auto;
}

.action-grid-btn {
  background: rgba(var(--army-khaki-rgb), 0.04);
  border: 1px solid rgba(var(--army-khaki-rgb), 0.4);
  color: #d1d1d1;
  font-family: var(--font-main);
  padding: 15px;
  cursor: pointer;
  text-transform: uppercase;
  letter-spacing: 1px;
  transition: all 0.2s;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  gap: 5px;
}

.action-grid-btn:hover {
  background: rgba(var(--army-khaki-rgb), 0.15);
  border-color: var(--army-khaki);
  color: var(--army-sand);
  box-shadow: 0 0 10px rgba(var(--army-khaki-rgb), 0.2);
}

.action-grid-btn span {
  font-size: 11px;
  color: var(--army-sage);
}

/* ==========================================
   CYBERPUNK MATRIX MODAL INFRASTRUCTURE
   ========================================== */

.matrix-modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100vw;
  height: 100vh;
  background: rgba(18, 23, 18, 0.88);
  backdrop-filter: blur(8px) contrast(110%);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 9999;
  opacity: 0;
  pointer-events: none;
  transition: all 0.3s cubic-bezier(0.19, 1, 0.22, 1);
}

.matrix-modal-overlay.active {
  opacity: 1;
  pointer-events: auto;
}

.matrix-modal-overlay.active .matrix-modal-content {
  transform: scale(1);
  box-shadow: 0 0 20px rgba(var(--army-olive-rgb), 0.1), inset 0 0 15px rgba(var(--army-olive-rgb), 0.05);
}

.matrix-modal-content {
  background: #151a15;
  border: 1px solid var(--army-olive);
  width: 100%;
  max-width: 460px;
  padding: 30px;
  font-family: 'Courier New', Courier, monospace;
  position: relative;
  transform: scale(0.95);
  transition: transform 0.3s cubic-bezier(0.19, 1, 0.22, 1);
  background-image: linear-gradient(rgba(var(--army-olive-rgb), 0.04) 50%, rgba(0, 0, 0, 0) 50%);
  background-size: 100% 4px;
}

.matrix-modal-content::before,
.matrix-modal-content::after {
  content: '';
  position: absolute;
  width: 12px;
  height: 12px;
  border-color: var(--army-olive);
  border-style: solid;
  pointer-events: none;
}
.matrix-modal-content::before { top: -3px; left: -3px; border-width: 3px 0 0 3px; }
.matrix-modal-content::after { bottom: -3px; right: -3px; border-width: 0 3px 3px 0; }

.modal-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 2px solid rgba(var(--army-olive-rgb), 0.2);
  padding-bottom: 12px;
  margin-bottom: 25px;
}

.modal-header h3 {
  color: var(--army-olive);
  margin: 0;
  font-size: 1.1rem;
  letter-spacing: 2px;
  text-shadow: 0 0 4px rgba(var(--army-olive-rgb), 0.4);
  font-weight: 700;
}

.close-modal-btn {
  background: transparent;
  border: 1px solid rgba(var(--army-red-rgb), 0.3);
  color: var(--army-red);
  cursor: pointer;
  font-size: 0.85rem;
  padding: 4px 8px;
  transition: all 0.2s ease;
  text-transform: uppercase;
}

.close-modal-btn:hover {
  background: var(--army-red);
  color: #111;
  box-shadow: 0 0 8px rgba(var(--army-red-rgb), 0.4);
}

.input-group { display: flex; flex-direction: column; margin-bottom: 20px; }

.input-group label {
  color: rgba(var(--army-olive-rgb), 0.7);
  font-size: 0.75rem;
  margin-bottom: 6px;
  text-transform: uppercase;
  letter-spacing: 1.5px;
}

.input-group input {
  background: rgba(10, 15, 10, 0.6);
  border: 1px solid rgba(var(--army-olive-rgb), 0.3);
  color: var(--army-olive);
  padding: 12px;
  font-family: inherit;
  font-size: 0.9rem;
  transition: all 0.25s ease;
}

.input-group input:focus {
  outline: none;
  border-color: var(--army-olive);
  background: rgba(20, 26, 20, 0.8);
  box-shadow: 0 0 8px rgba(var(--army-olive-rgb), 0.15);
}

.input-group input::placeholder { color: rgba(var(--army-olive-rgb), 0.3); }

.primary-submit {
  width: 100%;
  padding: 14px;
  background: transparent;
  border: 1px solid var(--army-olive);
  color: var(--army-olive);
  text-transform: uppercase;
  font-weight: bold;
  letter-spacing: 2px;
  cursor: pointer;
  position: relative;
  transition: all 0.2s ease;
  overflow: hidden;
  margin-top: 10px;
}

.primary-submit:hover {
  background: var(--army-olive);
  color: #111;
  box-shadow: 0 0 10px rgba(var(--army-olive-rgb), 0.4);
}

.primary-submit:active {
  background: rgba(var(--army-olive-rgb), 0.7);
  transform: scale(0.99);
}

.hidden {
  display: none !important;
}

/* Responsive adjustments for screens smaller than 900px */
@media (max-width: 900px) {
  body {
    overflow-y: auto; /* Allow scrolling on mobile */
    height: auto;     /* Remove fixed height */
  }

  main {
    display: flex;
    flex-direction: column;
    height: auto;
    padding: 10px;
    grid-template-rows: auto;
    gap: 15px;
  }

  header {
    height: auto;
    padding: 15px;
    flex-wrap: wrap;
    gap: 10px;
  }

  /* Reset fixed widths to allow fluid scaling */
  .panel, .avatar-wrapper {
    width: 100% !important;
    height: auto !important;
  }
  
  /* Prevent avatar from becoming too tall on wide mobile phones */
  .avatar-wrapper {
    max-height: 300px;
  }
}

</style>
</head>
<body>

<canvas id="matrix-canvas"></canvas>
<div id="crt-overlay"></div>
<div class="scanline"></div>

<main>
  <header style="grid-row: 1;">
    <div id="news-header" class="header-title glitch" data-text="LOADING...">LOADING...</div>
    <div class="nav-container">
      <button class="arrow prev" aria-label="Previous"></button>
      <button class="arrow next" aria-label="Next"></button>
    </div>
    <button id="openLoginBtn" class="uplink-btn">AUTHENTICATE</button>
    <button class="uplink-btn" onclick="initUplink()">NEW PROFILE</button>
    <button class="uplink-btn" onclick="connectInternet()">INTERNET</button>
    <button id="openModalBtn" class="uplink-btn hidden">INITIATE OVERRIDE</button>
  </header>

  <section class="panel" style="grid-column: 1; grid-row: 2;">
    <div class="panel-title">
    <span>PROFILE</span>
    <button class="modify-btn hidden" data-route="/api/profile/edit" id="intel_modify" aria-label="Modify">
        <svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round">
            <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
            <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
        </svg>
    </button>
    </div>
    
    <div class="avatar-wrapper">
      <img class="avatar-img" id="profile-picture" src="" alt="Operative Realframe Uplink">
      <div class="avatar-overlay"></div>
      <div class="avatar-bracket bracket-tl"></div>
      <div class="avatar-bracket bracket-tr"></div>
      <div class="avatar-bracket bracket-bl"></div>
      <div class="avatar-bracket bracket-br"></div>
    </div>
    
    <div class="intel-scroll-container">
      <div class="p-row"><span class="p-label">OPERATIVE:</span> <span class="p-val" id="profile-name">[ LOADING... ]</span></div>
      <div class="p-row"><span class="p-label">HANDLE:</span> <span class="p-val" id="profile-handle" style="color:var(--neon-yellow)">[ LOADING... ]</span></div>
      <div class="p-row"><span class="p-label">ASSIGNMENT:</span> <span class="p-val" id="profile-title">[ LOADING... ]</span></div>
      <div class="p-row"><span class="p-label">LOCATION:</span> <span class="p-val" id="profile-location">[ LOADING... ]</span></div>
      <div class="p-row" style="margin-top:10px;"><span class="p-label">MEM_SUMMARY:</span></div>
      <p id="profile-summary" style="font-size:11px; color:#aaa; line-height:1.4; border:1px dashed rgba(0,255,255,0.2); padding:8px; background:rgba(0,0,0,0.4);">[ LOADING... ]</p>
    </div>
  </section>

  <section class="panel" id="center-console" style="grid-column: 2; grid-row: 2; position: relative;">
  <button class="graph-toggle" id="console-toggle" title="Toggle System Matrix Overlay"></button>
  
  <canvas id="graph"></canvas>
  <div class="target-crosshair"></div>

  <div class="console-actions-panel">
    
    <div id="dynamic-actions-wrapper" style="display: contents;"></div>

    <button class="action-grid-btn add-new-btn" onclick="openSubProjectModal()">
      ＋ Add Sub-Project
      <span>[ADD]</span>
    </button>
  </div>
</section>

<section class="panel" style="grid-column: 3; grid-row: 2;">
    <div class="panel-title">
        <span>TRANSFERABLE SKILLS</span>
        
        <div class="panel-actions">
            <button class="modify-btn hidden" data-route="/api/skills/add" id="skill_add" aria-label="Add">
                <svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round">
                    <line x1="12" y1="5" x2="12" y2="19"></line>
                    <line x1="5" y1="12" x2="19" y2="12"></line>
                </svg>
            </button>
            <button class="modify-btn hidden" data-route="/api/skills/edit" id="skill_modify" aria-label="Modify">
                <svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
                    <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
                </svg>
            </button>
        </div>
    </div>
    <div id="skills-list" style="overflow-y:auto; height:100%;"></div>
</section>


<section class="panel" style="grid-column: 1; grid-row: 3;">
    <div class="panel-title">
        <span>EXPERIENCES</span>
        
        <div class="panel-actions">
            <button class="modify-btn hidden" data-route="/api/experiences/add" id="experience_add" aria-label="Add">
                <svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round">
                    <line x1="12" y1="5" x2="12" y2="19"></line>
                    <line x1="5" y1="12" x2="19" y2="12"></line>
                </svg>
            </button>
            <button class="modify-btn hidden" data-route="/api/experiences/edit" id="experience_modify" aria-label="Modify">
                <svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
                    <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
                </svg>
            </button>
        </div>
    </div>
    <div id="exp-list" style="overflow-y:auto; height:100%;"></div>
</section>

<section class="panel" style="grid-column: 2; grid-row: 3;">
    <div class="panel-title">
        <span>PROJECTS</span>
        
        <div class="panel-actions">
            <button class="modify-btn hidden" data-route="/api/projects/add" id="project_add" aria-label="Add">
                <svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round">
                    <line x1="12" y1="5" x2="12" y2="19"></line>
                    <line x1="5" y1="12" x2="19" y2="12"></line>
                </svg>
            </button>
            <button class="modify-btn hidden" data-route="/api/projects/edit" id="project_modify" aria-label="Modify">
                <svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
                    <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
                </svg>
            </button>
        </div>
    </div>
    <div id="projects-list" style="overflow-y:auto; height:100%; display:grid; grid-template-columns:1fr 1fr; gap:10px;"></div>
</section>

<section class="panel" style="grid-column: 3; grid-row: 3;">
    <div class="panel-title">SEARCH</div>
    <div id="inspector-area" style="font-size:12px;">
      <span style='color:#555;'>[ rAdIo PaRaDiSe ]</span>
      <audio controls preload="none">
        <source src="http://stream.radioparadise.com/aac-128" type="audio/aac">
        Your browser does not support the audio element.
      </audio>
    </div>
</section>
</main>

<script>
  const header = document.getElementById('news-header');
  const eventSource = new EventSource('/api/news-stream'); // Points to Rust backend

  eventSource.onmessage = function(event) {
    const newHeadline = event.data;
    
    // Crucial step: Update BOTH simultaneously to keep the glitch layout intact
    header.innerText = newHeadline;
    header.setAttribute('data-text', newHeadline);
  };
</script>

<div id="editor-modal">
  <div class="editor-panel">
    <div class="editor-header">
      <div class="editor-title" id="editor-title-text">PROJECT // NOTES</div>
      <div style="color: var(--alert-red); animation: blink 2s infinite;">[ ENCRYPTED CHANNEL ]</div>
    </div>
    <textarea id="editor-textarea" spellcheck="false"></textarea>
    <div class="editor-controls">
      <button class="btn" onclick="closeEditor()">ABORT</button>
      <button class="btn btn-save" id="btn-save" onclick="saveEditor()">COMMIT TO DATABANK</button>
    </div>
  </div>
</div>

<div id="sub-project-modal" class="matrix-modal-overlay" onclick="closeSubProjectModal(event)">
  <div class="matrix-modal-content" onclick="event.stopPropagation()">
    <div class="modal-header">
      <h3>PROVISION NEW MATRIX NODE</h3>
      <button class="close-modal-btn" onclick="closeSubProjectModal(event)">✕</button>
    </div>
    <form id="sub-project-form" onsubmit="saveSubProject(event)">
      <div class="input-group">
        <label>SubProject Name</label>
        <input type="text" id="subprojectname" placeholder="e.g., Integrity" required>
      </div>
      <div class="input-group">
        <label>Category of Sub-Project</label>
        <input type="text" id="subprojectcategory" placeholder="e.g., Valor" required>
      </div>
      <button type="submit" class="action-grid-btn primary-submit">Add Sub-Project</button>
    </form>
  </div>
</div>

<div id="dynamic-edit-modal" class="matrix-modal-overlay">
  <div class="matrix-modal-content">
    
    <div class="modal-header">
      <div style="display: flex; align-items: center; gap: 15px;">
        <h3>Edit Entry</h3>
        <span id="modal-record-counter" style="color: var(--army-khaki); font-size: 0.85rem; font-weight: bold;">[ ENTRY -- / -- ]</span>
      </div>
      
      <div style="display: flex; gap: 8px;">
        <button type="button" class="modal-nav-btn prev" style="background: transparent; border: 1px solid var(--army-olive); color: var(--army-olive); padding: 2px 8px; cursor: pointer; font-family: inherit;">&lt;</button>
        <button type="button" class="modal-nav-btn next" style="background: transparent; border: 1px solid var(--army-olive); color: var(--army-olive); padding: 2px 8px; cursor: pointer; font-family: inherit;">&gt;</button>
        <button type="button" class="close-modal-btn" onclick="closeModal()">[ Close ]</button>
      </div>
    </div>

    <form id="edit-form">
      <div id="form-fields"></div>
      <button type="submit" class="primary-submit">Save Current Record</button>
    </form>

  </div>
</div>

<div id="tacticalModal" class="matrix-modal-overlay">
  <div class="matrix-modal-content">
    
    <div class="modal-header">
      <h3>// SEC_OVERRIDE</h3>
      <button id="closeModalBtn" class="close-modal-btn">[X] ABORT</button>
    </div>

    <div id="statusConsole" class="exp-sum" style="margin-bottom: 20px; font-family: 'Courier New', monospace; font-size: 14px;">
      > AWAITING CREDENTIALS...
    </div>

    <form id="passwordForm">
      
      <div class="input-group">
        <label for="profileHandle">> TARGET_HANDLE</label>
        <div style="display: flex; gap: 10px;">
          <input type="text" id="profileHandle" placeholder="Enter profile handle..." required style="flex: 1;">
          <button type="button" id="verifyBtn" class="btn">VERIFY</button>
        </div>
      </div>

      <div class="input-group" id="passwordGroup" style="opacity: 0.4; pointer-events: none; transition: opacity 0.3s;">
        <label for="newPassword">> NEW_PASSWORD</label>
        <input type="password" id="newPassword" placeholder="Enter new designation..." disabled required>
      </div>

      <button type="submit" id="submitBtn" class="primary-submit" disabled style="opacity: 0.5; cursor: not-allowed;">
        COMMIT_CHANGES
      </button>

    </form>
  </div>
</div>

<div id="loginModal" class="matrix-modal-overlay">
  <div class="matrix-modal-content">
    <div class="modal-header">
      <h3>// AUTHENTICATION</h3>
      <button id="closeLoginBtn" class="close-modal-btn">[X]</button>
    </div>
    <form id="loginForm">
      <div class="input-group">
        <label for="loginHandle">> HANDLE</label>
        <input type="text" id="loginHandle" required>
      </div>
      <div class="input-group">
        <label for="loginPassword">> PASSWORD</label>
        <input type="password" id="loginPassword" required>
      </div>
      <button type="submit" class="primary-submit">INITIATE_HANDSHAKE</button>
    </form>
  </div>
</div>

<script>
// GLOBALS
let CURRENT_PROJECT_ID = "";
let CURRENT_PROJECT_NAME = "";
let CURRENT_PROFILE_HANDLE = "";

// Modal Window State Controls
function openSubProjectModal() {
    document.getElementById('sub-project-modal').classList.add('active');
}

function closeSubProjectModal(event) {
    if (event) event.preventDefault();
    document.getElementById('sub-project-modal').classList.remove('active');
    document.getElementById('sub-project-form').reset();
}

/**
 * Fetches sub-projects from the database API and renders them
 * @param {number} projectId - The ID of the parent project
 * @param {string} profileHandle - The active user profile handle
 */
async function loadSubProjects(projectId, profileHandle) {
    const container = document.getElementById('dynamic-actions-wrapper');
    container.innerHTML = '';

    try {
        // Replace with your actual backend endpoint routing
        const response = await fetch(`/api/subprojects?project_id=${projectId}&profile_handle=${encodeURIComponent(profileHandle)}`);
        if (!response.ok) throw new Error('Failed to synchronize console matrix.');

        const subProjects = await response.json();
        container.innerHTML = ''; // Clear loading state

        subProjects.forEach(sub => {
            const button = document.createElement('button');
            button.className = 'action-grid-btn';
            
            // Reconstruct the dynamic execution actions safely
            button.onclick = () => {
                    openEditor("projects", sub.project_id, sub.project_name, sub.subproject_name);
            };

            button.innerHTML = `
                ${sub.subproject_name}
                <span>${sub.subproject_category}</span>
            `;

            container.appendChild(button);
        });

    } catch (error) {
        console.error('Matrix Init Error:', error);
        container.innerHTML = '<p class="error">System Link Failure</p>';
    }
}

// Intercept, Save to Database, Close Window and Refresh Console
async function saveSubProject(event) {
    event.preventDefault();

    // Construct payload object matching NewSubProjectQuery struct fields in Rust
    const payload = {
        project_id: CURRENT_PROJECT_ID, 
        project_name: CURRENT_PROJECT_NAME,
        profile_handle: CURRENT_PROFILE_HANDLE,
        subproject_name: document.getElementById('subprojectname').value,
        subproject_category: document.getElementById('subprojectcategory').value
    };

    try {
        // Clean URL route passing data safely inside the body as JSON string
        const response = await fetch('/api/newsubprojects', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (!response.ok) throw new Error('Database rejection.');

        // Success: Clean up workspace
        closeSubProjectModal(); 
        
        // Refresh the console grid display list automatically
        await loadSubProjects(payload.project_id, payload.profile_handle); 

    } catch (error) {
        console.error('Submission Error:', error);
        if (typeof systemAlert === 'function') {
            systemAlert('Data synchronization failure.');
        }
    }
}

async function initUplink() {
  try {
    // 1. Fetch the pre-styled HTML from your Axum endpoint
    const response = await fetch('/api/uplink');
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    
    const fullHtml = await response.text();

    // 2. Open the pop-up window
    const popup = window.open("", "UplinkWindow", "width=600,height=400,scrollbars=yes");
    
    // 3. Directly stream the server's HTML content
    popup.document.open();
    popup.document.write(fullHtml);
    popup.document.close(); 

  } catch (err) {
    console.error("Uplink failed:", err);
    alert("FATAL UPLINK ERROR: Connection refused.");
  }
}

async function connectInternet() {
  try {
    // 1. Fetch the pre-styled HTML from your Axum endpoint
    const response = await fetch('/connect');
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    
    const fullHtml = await response.text();

    // 2. Open the pop-up window
    const popup = window.open("", "Internet", "width=600,height=400,scrollbars=yes");
    
    // 3. Directly stream the server's HTML content
    popup.document.open();
    popup.document.write(fullHtml);
    popup.document.close(); 

  } catch (err) {
    console.error("Uplink failed:", err);
    alert("FATAL UPLINK ERROR: Connection refused.");
  }
}


let skills = [];
let selectedNode = null;

const canvas = document.getElementById('matrix-canvas');
const ctx = canvas.getContext('2d');
let columns = [];

function resizeCanvas() {
  canvas.width = window.innerWidth;
  canvas.height = window.innerHeight;
  columns = Array(Math.floor(canvas.width / 14)).fill(0);
}
window.addEventListener('resize', resizeCanvas);
resizeCanvas();

function drawMatrix() {
  ctx.fillStyle = 'rgba(3, 4, 5, 0.04)';
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = '#0ff';
  ctx.font = '13px monospace';
  
  columns.forEach((y, i) => {
    const text = String.fromCharCode(33 + Math.floor(Math.random() * 93));
    ctx.fillText(text, i * 14, y);
    if (y > 100 + Math.random() * 10000) columns[i] = 0;
    else columns[i] = y + 14;
  });
}
setInterval(drawMatrix, 33);

let currentProfileIndex = 0;
let profiles = [];
let currentActiveRoute = '';

document.addEventListener('click', (e) => {
    // 1. Handle Arrow navigation
    if (e.target.classList.contains('arrow')) {
        if (e.target.classList.contains('prev')) {
            currentProfileIndex = (currentProfileIndex - 1 + profiles.length) % profiles.length;
        } else if (e.target.classList.contains('next')) {
            currentProfileIndex = (currentProfileIndex + 1) % profiles.length;
        }
        
        const consoleContainer = document.getElementById('center-console');
        if (consoleContainer) consoleContainer.classList.remove('collapsed');
        loadDashboard(currentProfileIndex, profiles[currentProfileIndex].handle);
    } 
    
    // 2. Handle Modify Button clicks
    // Use .closest() to ensure it catches the click even if the user clicks the SVG/path inside the button
    const modifyBtn = e.target.closest('.modify-btn');
    if (modifyBtn) {
        const route = modifyBtn.getAttribute('data-route');
        if (route) {
            currentActiveRoute = route;
            openEditModal(route);
        }
    }

    if (e.target.closest('.modal-nav-btn')) {
        const btn = e.target.closest('.modal-nav-btn');
        
        if (btn.classList.contains('prev')) {
            currentModalIndex = (currentModalIndex - 1 + modalRecords.length) % modalRecords.length;
        } else if (btn.classList.contains('next')) {
            currentModalIndex = (currentModalIndex + 1) % modalRecords.length;
        }
        
        renderCurrentModalRecord();
    }
});

document.getElementById('edit-form').addEventListener('submit', async (e) => {
    e.preventDefault();

    // 1. Determine our current mode
    const isAdding = currentActiveRoute.endsWith('/add');

    // 2. Prevent early exit if we are adding the first record
    if (!isAdding && (!modalRecords || modalRecords.length === 0)) return;
    
    // 3. Initialize the payload
    // Clone the existing record if editing (safe practice), or create a blank object if adding
    let payloadRecord = isAdding ? {} : { ...modalRecords[currentModalIndex] };
    const inputs = e.target.querySelectorAll('input[name]');

    // 4. Map values and handle Rust's strict Serde types
    inputs.forEach(input => {
        const key = input.name;
        const rawValue = input.value.trim();
        
        // Find a reference type: Check the existing record, fallback to a template, or check HTML input type
        const referenceRecord = (modalRecords && modalRecords.length > 0) ? modalRecords[0] : {};
        const originalType = isAdding ? typeof referenceRecord[key] : typeof payloadRecord[key];

        if (Array.isArray(referenceRecord[key])) {
            payloadRecord[key] = rawValue ? rawValue.split(',').map(item => item.trim()) : [];
        } else if (originalType === 'number' || input.type === 'number') {
            if (rawValue === '') {
                payloadRecord[key] = 0;
            } else if (rawValue.includes('.')) {
                payloadRecord[key] = parseFloat(rawValue);
            } else {
                payloadRecord[key] = parseInt(rawValue, 10);
            }
        } else {
            payloadRecord[key] = rawValue;
        }
    });

    // 5. Ensure relational IDs are attached
    if (!payloadRecord.profile_handle && !payloadRecord.handle) {
        payloadRecord["profile_handle"] = CURRENT_PROFILE_HANDLE;   
    }

    // 6. Determine routing (Assuming your Rust backend uses /add for inserts and /update for edits)
    const fetchRoute = currentActiveRoute.replace(/\/(edit|add)/, '/update');

    try {
        const submitBtn = e.target.querySelector('.primary-submit');
        const originalText = submitBtn.innerText;
        submitBtn.innerText = '[ TRANSMITTING TYPED DATA... ]';
        submitBtn.disabled = true;

        const response = await fetch(fetchRoute, {
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payloadRecord)
        });

        if (!response.ok) throw new Error('Uplink synchronization failed');

        submitBtn.style.borderColor = 'var(--army-sage)';
        submitBtn.innerText = '[ DATA LINK SECURED ]';
        
        // 7. Update frontend state immediately on success so the UI doesn't desync
        if (isAdding) {
            if (!modalRecords) modalRecords = [];
            modalRecords.push(payloadRecord);
            currentModalIndex = modalRecords.length - 1; // Focus the new record
        } else {
            modalRecords[currentModalIndex] = payloadRecord;
        }
        
        setTimeout(() => {
            submitBtn.innerText = originalText;
            submitBtn.disabled = false;
            submitBtn.style.borderColor = '';
            
            // ─── IN-PLACE GRAPHICS REWORK ───
            syncDashboardUI(currentActiveRoute, payloadRecord);
            closeModal(); 
        }, 1200);

    } catch (error) {
        console.error("Transmission Failure:", error);
        alert("[ AXUM NODE REJECTED PAYLOAD: TYPE MISMATCH DETECTED ]");
        
        const submitBtn = e.target.querySelector('.primary-submit');
        submitBtn.innerText = 'Save Current Record';
        submitBtn.disabled = false;
    }
});

document.addEventListener('DOMContentLoaded', () => {
  
  // ==========================================
  // 1. MODAL UTILITY HELPERS
  // ==========================================
  
  // Reusable function to bind open, close, and reset events to any modal
  const initModal = (openBtnId, modalId, closeBtnId, onOpenCallback, onCloseCallback) => {
    const openBtn = document.getElementById(openBtnId);
    const modal = document.getElementById(modalId);
    const closeBtn = document.getElementById(closeBtnId);
    
    if (!openBtn || !modal || !closeBtn) return;
    
    openBtn.addEventListener('click', () => {
      modal.classList.add('active');
      if (onOpenCallback) onOpenCallback();
    });
    
    closeBtn.addEventListener('click', () => {
      modal.classList.remove('active');
      // Wait for CSS transition (300ms) before executing cleanup
      if (onCloseCallback) setTimeout(onCloseCallback, 300);
    });
  };

  // ==========================================
  // 2. AUTHENTICATION (LOGIN) LOGIC
  // ==========================================
  
  const loginForm = document.getElementById('loginForm');
  const openLoginBtn = document.getElementById('openLoginBtn');
  const loginModal = document.getElementById('loginModal');
  
  // Array of element IDs to reveal upon successful authentication
  const secureElementsIds = [
    'openModalBtn', 'intel_modify', 'skill_add', 'skill_modify', 
    'experience_add', 'experience_modify', 'project_add', 'project_modify'
  ];

  // Initialize Login Modal
  initModal('openLoginBtn', 'loginModal', 'closeLoginBtn');

  if (loginForm) {
    loginForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      
      const handle = document.getElementById('loginHandle').value;
      const password = document.getElementById('loginPassword').value;

      try {
        const response = await fetch('/api/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ profile_handle: handle, password })
        });

        if (response.ok) {
          console.log("Authentication granted.");
          
          // Hide Login Trigger
          if (openLoginBtn) openLoginBtn.classList.add('hidden');
          
          // Reveal Secure UI Elements safely
          secureElementsIds.forEach(id => {
            const el = document.getElementById(id);
            if (el) el.classList.remove('hidden');
          });
          
          // Close Modal & Notify
          loginModal.classList.remove('active');
          alert("ACCESS GRANTED: UPLINK ESTABLISHED");
        } else {
          alert("ACCESS DENIED: INVALID CREDENTIALS");
        }
      } catch (err) {
        console.error("Auth error:", err);
      }
    });
  }

  // ==========================================
  // 3. PASSWORD MODIFICATION LOGIC
  // ==========================================
  
  const passwordForm = document.getElementById('passwordForm');
  const verifyBtn = document.getElementById('verifyBtn');
  const profileHandleInput = document.getElementById('profileHandle');
  const newPasswordInput = document.getElementById('newPassword');
  const passwordGroup = document.getElementById('passwordGroup');
  const submitBtn = document.getElementById('submitBtn');
  const statusConsole = document.getElementById('statusConsole');
  const tacticalModal = document.getElementById('tacticalModal');
  
  let currentUserData = null;

  // Console output helper
  const logToConsole = (msg, colorVar) => {
    if (!statusConsole) return;
    statusConsole.textContent = `> ${msg}`;
    statusConsole.style.borderLeftColor = `var(${colorVar})`;
    statusConsole.style.color = `var(${colorVar})`;
  };

  // State cleanup helper
  const resetPasswordModal = () => {
    if (passwordForm) passwordForm.reset();
    currentUserData = null;
    
    if (passwordGroup) {
      passwordGroup.style.opacity = '0.4';
      passwordGroup.style.pointerEvents = 'none';
    }
    if (newPasswordInput) newPasswordInput.disabled = true;
    
    if (submitBtn) {
      submitBtn.disabled = true;
      submitBtn.style.opacity = '0.5';
      submitBtn.style.cursor = 'not-allowed';
    }
    
    logToConsole('AWAITING CREDENTIALS...', '--army-khaki');
  };

  // Initialize Tactical Modal (pass the reset function to execute on close)
  initModal('openModalBtn', 'tacticalModal', 'closeModalBtn', null, resetPasswordModal);

  // Phase 1: Verify User via GET
  if (verifyBtn) {
    verifyBtn.addEventListener('click', async () => {
      const handle = profileHandleInput.value.trim();
      
      if (!handle) {
        return logToConsole('ERROR: HANDLE REQUIRED', '--army-red');
      }

      logToConsole('FETCHING PROFILE DATA...', '--army-sand');

      try {
        const response = await fetch(`/api/password?profile_handle=${encodeURIComponent(handle)}`);
        
        if (!response.ok) throw new Error(`STATUS ${response.status}`);

        currentUserData = await response.json();
        logToConsole('PROFILE VERIFIED. ENTER NEW DESIGNATION.', '--army-sage');
        
        // Unlock Phase 2 UI
        passwordGroup.style.opacity = '1';
        passwordGroup.style.pointerEvents = 'auto';
        newPasswordInput.disabled = false;
        
        submitBtn.disabled = false;
        submitBtn.style.opacity = '1';
        submitBtn.style.cursor = 'pointer';
        
        newPasswordInput.focus();

      } catch (error) {
        logToConsole(`VERIFICATION FAILED: ${error.message}`, '--army-red');
        currentUserData = null;
      }
    });
  }

  // Phase 2: Commit Password Change via POST
  if (passwordForm) {
    passwordForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      if (!currentUserData) return;

      logToConsole('COMMITTING TRANSACTION...', '--army-sand');

      const payload = {
        ...currentUserData,
        password: newPasswordInput.value 
      };

      try {
        const response = await fetch('/api/password/change', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        if (!response.ok) {
          const errorText = await response.text();
          throw new Error(errorText || `STATUS ${response.status}`);
        }

        logToConsole('UPDATE SUCCESSFUL. TRANSACTION CLOSED.', '--army-sage');
        
        // Auto-close modal after success
        setTimeout(() => {
          tacticalModal.classList.remove('active');
          setTimeout(resetPasswordModal, 300); // Reset state after closing
        }, 2000);

      } catch (error) {
        logToConsole(`UPDATE FAILED: ${error.message}`, '--army-red');
      }
    });
  }
});


function syncDashboardUI(route, record) {
    const isAdding = route.endsWith('/add');

    // ─── SCENARIO 1: Core Operative Profile ───
    if (route.includes('/profile')) {
        if (record.picture) document.getElementById('profile-picture').src = record.picture;
        if (record.name)    document.getElementById('profile-name').innerText = record.name;
        
        const activeHandle = record.handle || record.profile_handle;
        if (activeHandle)   document.getElementById('profile-handle').innerText = `[${activeHandle}]`;
        
        if (record.title)    document.getElementById('profile-title').innerText = record.title;
        if (record.location) document.getElementById('profile-location').innerText = record.location;
        if (record.summary)  document.getElementById('profile-summary').innerText = record.summary;
    }

    // ─── SCENARIO 2: Tactical Skills HUD Bars ───
    else if (route.includes('/skills')) {
        const cleanName = record.name ? record.name.replace(/'/g, "\\'") : '';
        
        if (isAdding) {
            let styleClass = '';
            if (record.score > 95) styleClass = 'critical';
            else if (record.score > 90) styleClass = 'warning';

            const newSkillHTML = `
                <div class="hud-bar-container ${styleClass}" data-id="${record.id}" onclick="openEditor('skills', '${record.id}', '${cleanName}')">
                    <div class="hud-bar-label">
                        <span>${record.name} [${record.category}]</span>
                        <span>${record.score}%</span>
                    </div>
                    <div class="hud-bar-bg"> 
                        <div class="hud-bar-fill" style="width: ${record.score}%;"></div>
                    </div>
                </div>
            `;
            document.getElementById('skills-list').insertAdjacentHTML('beforeend', newSkillHTML);
        } else {
            const targetElement = document.querySelector(`#skills-list .hud-bar-container[data-id="${record.id}"]`);
            if (targetElement) {
                targetElement.querySelector('.hud-bar-label span:first-child').innerText = `${record.name} [${record.category}]`;
                targetElement.querySelector('.hud-bar-label span:last-child').innerText = `${record.score}%`;
                targetElement.querySelector('.hud-bar-fill').style.width = `${record.score}%`;
                targetElement.setAttribute('onclick', `openEditor('skills', '${record.id}', '${cleanName}')`);
                
                targetElement.classList.remove('critical', 'warning');
                if (record.score > 95) targetElement.classList.add('critical');
                else if (record.score > 90) targetElement.classList.add('warning');
            }
        }
    }

    // ─── SCENARIO 3: Service Experiences Grid ───
    else if (route.includes('/experiences')) {
        if (isAdding) {
            const skillsTags = (record.skills || []).map(s => `<span class="tag">${s}</span>`).join('');
            
            const newExpHTML = `
                <div class="exp-card" data-id="${record.id}">
                    <div class="exp-title">${record.role}</div>
                    <div class="exp-org">
                        <span>${record.organization}</span>
                        <span>${record.years} YRS</span>
                    </div>
                    <div class="exp-sum">${record.summary}</div>
                    <div>${skillsTags}</div>
                </div>
            `;
            document.getElementById('exp-list').insertAdjacentHTML('afterbegin', newExpHTML);
        } else {
            const targetCard = document.querySelector(`#exp-list .exp-card[data-id="${record.id}"]`);
            if (targetCard) {
                targetCard.querySelector('.exp-title').innerText = record.role;
                
                const orgSpans = targetCard.querySelectorAll('.exp-org span');
                if (orgSpans[0]) orgSpans[0].innerText = record.organization;
                if (orgSpans[1]) orgSpans[1].innerText = `${record.years} YRS`;
                
                targetCard.querySelector('.exp-sum').innerText = record.summary;
                targetCard.querySelector('div:last-child').innerHTML = (record.skills || [])
                    .map(s => `<span class="tag">${s}</span>`).join('');
            }
        }
    }

    // ─── SCENARIO 4: Operations & Projects Grid ───
    else if (route.includes('/projects')) {
        const cleanEscapedName = record.name ? record.name.replace(/'/g, "\\'") : 'UNKNOWN';

        if (isAdding) {
            const newProjHTML = `
                <div class="proj-card" data-id="${record.id}" style="margin-bottom:0;" onclick="selectProjectContext(\`${record.id}\`, \`${cleanEscapedName}\`)">
                    <div class="exp-title" style="color:var(--neon-pink);">${record.name}</div>
                    <div class="exp-org" style="margin-bottom:4px;">
                        <span>IMPACT INDEX</span>
                        <span style="color:var(--neon-green)">${record.impact}%</span>
                    </div>
                    <div class="exp-sum" style="border-color:var(--neon-cyan); height: 50px; overflow:hidden; text-overflow:ellipsis;">
                        ${record.description}
                    </div>
                </div>
            `;
            document.getElementById('projects-list').insertAdjacentHTML('afterbegin', newProjHTML);
        } else {
            const targetCard = document.querySelector(`#projects-list .proj-card[data-id="${record.id}"]`);
            if (targetCard) {
                targetCard.querySelector('.exp-title').innerText = record.name;
                targetCard.querySelector('.exp-org span:last-child').innerText = `${record.impact}%`;
                targetCard.querySelector('.exp-sum').innerText = record.description;
                // Re-apply click handler using template literal backticks as seen in injection script
                targetCard.setAttribute('onclick', `selectProjectContext(\`${record.id}\`, \`${cleanEscapedName}\`)`);
            }
        }
    }
}

async function selectProjectContext(projectId, projectName) {
    // Dynamically rewrite states to track active element selection
    CURRENT_PROJECT_ID = projectId;
    CURRENT_PROJECT_NAME = projectName;
    
    // Auto-reload the console dashboard view for the new project context
    await loadSubProjects(projectId, CURRENT_PROFILE_HANDLE);
}

async function loadDashboard(index = 0, handle = "N3_operative_001") {
  try {
    const res = await fetch(`/api/dashboard?handle=${encodeURIComponent(handle)}`);
    if (!res.ok) {
        throw new Error(`Server returned HTTP ${res.status}: ${res.statusText}`);
    }
    const data = await res.json();

    profiles = data.profiles;

    let trueIndex = data.profiles.findIndex(p => p.handle === handle.replace('@',''));
    if (trueIndex === -1) trueIndex = 0; // Fallback security check

    const profile = data.profiles[trueIndex];

    CURRENT_PROFILE_HANDLE = profile.handle;
    
    // Bind core profile identifiers
    document.getElementById('profile-picture').src = profile.picture;
    document.getElementById('profile-name').innerText = profile.name;
    document.getElementById('profile-handle').innerText = `[${profile.handle}]`;
    document.getElementById('profile-title').innerText = profile.title;
    document.getElementById('profile-location').innerText = profile.location;
    document.getElementById('profile-summary').innerText = profile.summary;
    
    skills = data.skills[0];
    
    // 1. Inject Skill Metrics Matrix (with fixed click triggers)
    const skList = document.getElementById('skills-list');
    skList.innerHTML = skills.map(s => {
    let catClass = '';
    if(s.score > 95) catClass = 'critical';
    else if(s.score > 90) catClass = 'warning';
    return `
        <div class="hud-bar-container ${catClass}" data-id="${s.id}" onclick="openEditor('skills', '${s.id}', '${s.name}')">
        <div class="hud-bar-label"><span>${s.name} [${s.category}]</span><span>${s.score}%</span></div>
        <div class="hud-bar-bg"><div class="hud-bar-fill" style="width: ${s.score}%"></div></div>
        </div>
    `;
    }).join('');

    experiences = data.experiences[0];

    // 2. Inject Career Nodes
    const expList = document.getElementById('exp-list');
    expList.innerHTML = experiences.map(e => `
    <div class="exp-card" data-id="${e.id}">
        <div class="exp-title">${e.role}</div>
        <div class="exp-org"><span>${e.organization}</span><span>${e.years} YRS</span></div>
        <div class="exp-sum">${e.summary}</div>
        <div>${e.skills.map(s => `<span class="tag">${s}</span>`).join('')}</div>
    </div>
    `).join('');

    projects = data.projects[0];

    CURRENT_PROJECT_ID = projects[0].id;
    CURRENT_PROJECT_NAME = projects[0].name;

    if (projects && projects.length > 0) {
        CURRENT_PROJECT_ID = projects[0].id;
        CURRENT_PROJECT_NAME = projects[0].name;
    }

    await loadSubProjects(CURRENT_PROJECT_ID, CURRENT_PROFILE_HANDLE);

    
       

    // 3. Inject Project Grid Files (with polymorphic context argument matched)
    const projList = document.getElementById('projects-list');
    projList.innerHTML = projects.map(p => {
    const escapedName = p.name.replace(/'/g, "\\'");
    
    return `
        <div class="proj-card" data-id="${p.id}" style="margin-bottom:0;" onclick="selectProjectContext(\`${p.id}\`, \`${escapedName}\`)">
        <div class="exp-title" style="color:var(--neon-pink);">${p.name}</div>
        <div class="exp-org" style="margin-bottom:4px;">
            <span>IMPACT INDEX</span>
            <span style="color:var(--neon-green)">${p.impact}%</span>
        </div>
        <div class="exp-sum" style="border-color:var(--neon-cyan); height: 50px; overflow:hidden; text-overflow:ellipsis;">
            ${p.description}
        </div>
        </div>
    `;
    }).join('');

    // Boot interactive central graphics wireframe
    initGraph(skills);

  } catch (e) {
    console.error("Uplink dropped. Re-routing initialization diagnostics.", e);
    document.getElementById('profile-name').innerText = "ERR_CONNECTION";
    document.getElementById('profile-summary').innerHTML = `<span style="color:var(--alert-red); font-weight:bold;">FATAL UPLINK ERROR:</span> ${e.message}<br><br>Check your browser console (F12) and ensure the Axum server is actively running.`;
  }
}


const gCanvas = document.getElementById('graph');
const gCtx = gCanvas.getContext('2d');
let graphNodes = [];
let animationFrameId;
let mouse = { x: -1000, y: -1000 };

function resizeGraph() {
  if (!gCanvas.parentElement) return;
  const rect = gCanvas.parentElement.getBoundingClientRect();
  gCanvas.width = rect.width;
  gCanvas.height = rect.height;
}
window.addEventListener('resize', resizeGraph);

function initGraph(skillsData) {
  if (animationFrameId) cancelAnimationFrame(animationFrameId);
  resizeGraph();

  const cx = gCanvas.width / 2;
  const cy = gCanvas.height / 2;

  graphNodes = skillsData.map((s) => ({
    id: s.id,
    label: s.name,
    x: cx + (Math.random() - 0.5) * 100,
    y: cy + (Math.random() - 0.5) * 100,
    vx: 0,
    vy: 0,
    links: s.links,
    size: 4 + (s.score / 25), 
    score: s.score
  }));

  graphNodes.forEach(node => {
    node.connectedNodes = node.links
      .map(targetId => graphNodes.find(n => n.label === targetId))
      .filter(Boolean); 
  });

  gCanvas.addEventListener('mousemove', (e) => {
    const r = gCanvas.getBoundingClientRect();
    mouse.x = e.clientX - r.left;
    mouse.y = e.clientY - r.top;
  });

  gCanvas.addEventListener('mouseleave', () => {
    mouse.x = -1000; mouse.y = -1000;
    if (selectedNode) {
      selectedNode = null;
      updateInspector(null); 
    }
  });

  gCanvas.addEventListener('click', () => {
    if (selectedNode) {
      openEditor('skills', selectedNode.id, selectedNode.label);
    }
  });

  // BIND TOGGLE EVENT SAFELY ONCE INSIDE INITIALIZER
  const toggleBtn = document.getElementById('console-toggle');
  if (toggleBtn && !toggleBtn.dataset.bound) {
    toggleBtn.addEventListener('click', () => {
      document.getElementById('center-console').classList.toggle('collapsed');
    });
    toggleBtn.dataset.bound = "true"; // Prevents multiple bindings on switch
  }

  drawGraph();
}

function applyPhysics() {
  const cx = gCanvas.width / 2;
  const cy = gCanvas.height / 2;
  
  const REPULSION = 1500; 
  const SPRING_STIFFNESS = 0.05; 
  const SPRING_LENGTH = 80; 
  const DAMPING = 0.85; 
  const CENTER_GRAVITY = 0.02; 

  let closestNode = null; 
  let minMouseDist = 80; // The magnetic "catch" radius of the cursor

  // Pass 1: Find the closest node to the mouse uplink
  for (let i = 0; i < graphNodes.length; i++) {
    let n = graphNodes[i];
    const distMouse = Math.hypot(n.x - mouse.x, n.y - mouse.y);
    if (distMouse < minMouseDist) {
      minMouseDist = distMouse;
      closestNode = n;
    }
  }

  // Pass 2: Apply physical forces
  for (let i = 0; i < graphNodes.length; i++) {
    let n1 = graphNodes[i];

    n1.vx += (cx - n1.x) * CENTER_GRAVITY;
    n1.vy += (cy - n1.y) * CENTER_GRAVITY;

    const distMouse = Math.hypot(n1.x - mouse.x, n1.y - mouse.y);
    
    // Magnetic ICE Barrier: Pushes nodes away, but weakens if it's the active target
    if (distMouse < 120) {
      let defense = (n1 === closestNode) ? 0.01 : 0.06; // Target node gets caught, others get pushed hard
      const force = (120 - distMouse) * defense;
      n1.vx += ((n1.x - mouse.x) / distMouse) * force;
      n1.vy += ((n1.y - mouse.y) / distMouse) * force;
    }

    // Node Repulsion
    for (let j = i + 1; j < graphNodes.length; j++) {
      let n2 = graphNodes[j];
      let dx = n1.x - n2.x;
      let dy = n1.y - n2.y;
      let dist = Math.hypot(dx, dy) || 1; 

      let force = REPULSION / (dist * dist);
      let fx = (dx / dist) * force;
      let fy = (dy / dist) * force;

      n1.vx += fx; n1.vy += fy;
      n2.vx -= fx; n2.vy -= fy;
    }

    // Synaptic Spring Attraction (Links)
    n1.connectedNodes.forEach(n2 => {
      let dx = n2.x - n1.x;
      let dy = n2.y - n1.y;
      let dist = Math.hypot(dx, dy) || 1;
      
      let force = (dist - SPRING_LENGTH) * SPRING_STIFFNESS;
      let fx = (dx / dist) * force;
      let fy = (dy / dist) * force;

      n1.vx += fx; n1.vy += fy;
      n2.vx -= fx; n2.vy -= fy; 
    });
  }

  // Update Inspector UI state smoothly
  if (closestNode !== selectedNode) {
    selectedNode = closestNode;
    if (typeof updateInspector === "function") {
      updateInspector(selectedNode);
    }
  }

  // Apply Velocity & Friction
  graphNodes.forEach(n => {
    n.vx *= DAMPING;
    n.vy *= DAMPING;
    n.x += n.vx;
    n.y += n.vy;
  });
}

const cityLayers = [
  createCityLayer(40, 0.15, 0.25),
  createCityLayer(25, 0.35, 0.5),
  createCityLayer(15, 0.75, 1.0)
];

function createCityLayer(count, minHeightRatio, maxHeightRatio) {
  const buildings = [];
  let x = 0;
  for (let i = 0; i < count; i++) {
    const w = 40 + Math.random() * 80;
    const h = window.innerHeight * (minHeightRatio + Math.random() * (maxHeightRatio - minHeightRatio));
    buildings.push({ x, width: w, height: h });
    x += w + 10;
  }
  return { width: x, buildings };
}

function drawCityFrame(ctx, width, height) {
  const time = Date.now() * 0.0001;
  const gradient = ctx.createLinearGradient(0, 0, 0, height);
  gradient.addColorStop(0, '#050814');
  gradient.addColorStop(0.5, '#0a1025');
  gradient.addColorStop(1, '#030508');
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, width, height);

  cityLayers.forEach((layer, layerIndex) => {
    const speed = (layerIndex + 1) * 20;
    const offset = (time * speed) % layer.width;
    ctx.fillStyle = `rgba(0,255,255,${0.08 + layerIndex * 0.08})`;

    for (let repeat = -1; repeat <= 1; repeat++) {
      layer.buildings.forEach(b => {
        const x = b.x - offset + repeat * layer.width;
        const y = height - b.height;
        
        ctx.fillRect(x, y, b.width, b.height);
        ctx.strokeStyle = `rgba(0,255,255,${0.3 + layerIndex * 0.2})`;
        ctx.strokeRect(x, y, b.width, b.height);
        
        // Window logic optimized: Only render if building is visible
        if (x > -b.width && x < width) {
           ctx.fillStyle = `rgba(0,255,255,0.2)`;
           ctx.fillRect(x + 5, y + 10, b.width - 10, 5);
        }
      });
    }
  });

  // Perspective Grid
  ctx.strokeStyle = 'rgba(0,255,255,0.05)';
  for (let i = 0; i < 30; i++) {
    ctx.beginPath();
    ctx.moveTo(width / 2, height);
    ctx.lineTo((i / 30) * width, height * 0.4);
    ctx.stroke();
  }
}

/**
 * Main animation loop
 */
function drawGraph() {
  applyPhysics(); // Update positions

  // 1. Draw the static city background
  drawCityFrame(gCtx, gCanvas.width, gCanvas.height);

  // 2. Draw a semi-transparent layer over the city 
  // This creates the "dashboard" feel and allows motion trails
  gCtx.fillStyle = 'rgba(8, 10, 15, 0.25)'; 
  gCtx.fillRect(0, 0, gCanvas.width, gCanvas.height);

  // 3. Draw Synaptic Links
  gCtx.lineWidth = 1;
  graphNodes.forEach(n1 => {
    n1.connectedNodes.forEach(n2 => {
      const dist = Math.hypot(n1.x - n2.x, n1.y - n2.y);
      const alpha = Math.max(0.05, 1 - (dist / 200)); 
      gCtx.beginPath();
      gCtx.strokeStyle = `rgba(0, 255, 170, ${alpha})`; 
      gCtx.moveTo(n1.x, n1.y);
      gCtx.lineTo(n2.x, n2.y);
      gCtx.stroke();
    });
  });

  // 4. Draw Nodes
  graphNodes.forEach(n => {
    const isHovered = (n === selectedNode);
    gCtx.beginPath();
    gCtx.fillStyle = isHovered ? '#ff003c' : '#00ffaa'; 
    gCtx.arc(n.x, n.y, isHovered ? n.size * 1.5 : n.size, 0, Math.PI * 2);
    gCtx.fill();

    // Pulse
    if (isHovered || Math.random() > 0.98) {
      gCtx.beginPath();
      gCtx.strokeStyle = isHovered ? '#ff003c' : 'rgba(0, 255, 170, 0.5)';
      gCtx.arc(n.x, n.y, n.size * 2.5, 0, Math.PI * 2);
      gCtx.stroke();
    }

    // Label
    if (isHovered || n.score > 95) { 
      gCtx.fillStyle = isHovered ? '#fff' : 'rgba(0, 255, 170, 0.7)';
      gCtx.font = isHovered ? 'bold 12px monospace' : '10px monospace';
      gCtx.fillText(n.label, n.x + 10, n.y + 4);
    }
  });

  animationFrameId = requestAnimationFrame(drawGraph);
}

/*
* Search Box
*/

let registryAbortController = null;

// Fires on every single keystroke inside the search field
async function scanRemoteRegistry(query) {
    const resultsTray = document.getElementById('search-results-tray');
    const sanitizedQuery = query.trim();

    // 1. Clear and hide tray if input string is empty
    if (!sanitizedQuery) {
        resultsTray.innerHTML = "";
        resultsTray.style.display = "none";
        return;
    }

    // 2. Abort previous unfinished keystroke fetches to prevent race conditions
    if (registryAbortController) {
        registryAbortController.abort();
    }
    registryAbortController = new AbortController();

    try {
        const response = await fetch(`/api/profiles/search?q=${encodeURIComponent(sanitizedQuery)}`, {
            signal: registryAbortController.signal
        });

        if (!response.ok) throw new Error("Registry datalink dropped");
        const matches = await response.json();

        if (matches.length === 0) {
            resultsTray.innerHTML = "<div style='color:#ff3333; padding: 0.5rem;'>// NO MATCHING PROFILES FOUND</div>";
            resultsTray.style.display = "block";
            return;
        }

        // 3. Render matching profiles into the tray
        resultsTray.style.display = "block";
        resultsTray.innerHTML = matches.map((profile) => `
            <div class="search-result-item" 
                onclick="loadDashboard(null, '${profile.handle}')"
                style="border: 1px dashed #333; padding: 0.6rem; margin-bottom: 0.4rem; cursor: pointer; transition: all 0.2s ease; background: #090a0f;">
                <div style="display: flex; justify-content: space-between;">
                    <span style="color: var(--neon-yellow); font-size: 0.9rem; font-weight: bold;">@${profile.handle}</span>
                    <span style="color: #aaa; font-size: 0.8rem;">${profile.name || ''}</span>
                </div>
                <div style="color: #666; font-size: 0.75rem; margin-top: 2px;">${profile.title || 'Unassigned Title'}</div>
            </div>
        `).join('');

        // Apply interactive cyberpunk visual feedback states
        document.querySelectorAll('.search-result-item').forEach(item => {
            item.addEventListener('mouseenter', () => { item.style.borderColor = 'var(--neon-pink)'; item.style.background = 'rgba(255,0,234,0.03)'; });
            item.addEventListener('mouseleave', () => { item.style.borderColor = '#333'; item.style.background = '#090a0f'; });
        });

    } catch (err) {
        if (err.name !== 'AbortError') {
            console.error("Registry scan error:", err);
        }
    }
}

async function updateInspector(node) {
  const area = document.getElementById('inspector-area');
  
  // Guard clause if no node is provided
  if (!node) { 
    // Matrix initialized or reset loop
    area.innerHTML = `
  <div class="search-registry-box" style="border: 1px solid var(--neon-cyan); padding: 1.5rem; background: rgba(0,0,0,0.6); margin-bottom: 2rem;">
      <label style="color: var(--neon-cyan); font-size: 0.8rem; letter-spacing: 2px; display: block; margin-bottom: 0.5rem;">
          // LIVE_MATRIX_SEARCH
      </label>
      <div class="input-group" style="margin-bottom: 0;">
          <input type="text" id="registry-search-input" 
                placeholder="Type handle, name, or title to scan..." 
                oninput="scanRemoteRegistry(this.value)"
                style="width: 100%; box-sizing: border-box; font-size: 1rem; border-color: var(--neon-cyan);">
      </div>
      
      <div id="search-results-tray" style="max-height: 250px; overflow-y: auto; margin-top: 0.5rem; display: none;"></div>
  </div>`; 
      return; 
  }
  
  // 1. Safety Check: Ensure the skill actually exists in your data array
  const s = skills.find(sk => sk.id === node.id);
  if (!s) {
    area.innerHTML = "<span style='color:#ff5555;'>[ ERROR: NODE DATA UNRESOLVED ]</span>";
    return;
  }

  // 2. DECLARE IT HERE: Outer scope so it's accessible everywhere below
  let uplinkText = ""; 

  try {
    const res = await fetch(`/api/skills/${s.id}/notes`);
    if (!res.ok) throw new Error("Network response was not ok");
    
    const data = await res.json();
    uplinkText = data.text || "";
  } catch (e) {
    // Fallback plain string matches the data type above
    uplinkText = `NODE STABILIZED AND SECURED AT CORE EFFICIENCY LOAD INDEX RATE STATUS VALUE: OPTIMAL.`;
  }
  
  // 3. Render the UI safely
  area.innerHTML = `
    <div class="p-row"><span class="p-label">VECTOR:</span> <span class="p-val" style="color:var(--neon-yellow)">${s.category}</span></div>
    <div class="p-row"><span class="p-label">RATING:</span> <span class="p-val">${s.score}% DEPTH</span></div>
    <div class="p-row"><span class="p-label">SYNAPSES:</span> <span class="p-val">${s.links ? s.links.length : 0} EDGES</span></div>
    <div class="p-row" style="margin-top:10px;"><span class="p-label">UPLINK_INFO:</span></div>
    <div style="font-size:11px; color:#aaa; line-height:1.4; padding:6px; background:rgba(0,0,0,0.2); border:1px dashed #333;">
      ${uplinkText} </div>
  `;
}

window.onload = loadDashboard(0);

let currentEditId = null;
let currentEditType = null;
let currentRecordVersion = null; // Local copy version token track
let syncInterval = null;         // Background tracking thread handle
let isSaving = false;            // Execution guard
let isOutOfSync = false;         // Circuit-breaker conflict flag
let hasUnsavedChanges = false;   // Tracking local text buffer state
let subProjectName = null;

async function openEditor(type, id, name, subProjName) {
  currentEditId = id;
  currentEditType = type;
  isOutOfSync = false; 
  hasUnsavedChanges = false;
  subProjectName = subProjName;
  
  const prefix = type === 'skills' ? 'SKILL' : 'PROJECT';
  document.getElementById('editor-title-text').innerText = `${prefix} // ${name} // DETAILS`;
  document.getElementById('editor-modal').style.display = 'flex';
  
  const textarea = document.getElementById('editor-textarea');
  textarea.value = "[ RETRIEVING MEMORY BLOCK... ]";
  textarea.disabled = true;

  const btn = document.getElementById('btn-save');
  btn.disabled = false;
  btn.style.borderColor = ""; // Reset custom styles
  btn.innerText = "SEND";

  let res;

  try {
    if (type === "skills") {
        res = await fetch(`/api/skills/${id}/notes`);
    } else {
        res = await fetch(`/api/projects/${id}/subprojects/${encodeURIComponent(subProjectName)}/notes`);
    }
    const data = await res.json();
    textarea.value = data.text;
    currentRecordVersion = data.version; 
  } catch (e) {
    textarea.value = `ERR_SECTOR_UNREADABLE: ${e.message}`;
  }
  
  textarea.disabled = false;
  textarea.focus();

  // Spin up real-time telemetry check
  startBackgroundSync();
}

function closeEditor() {
  document.getElementById('editor-modal').style.display = 'none';
  currentEditId = null;
  currentEditType = null;
  if (syncInterval) {
    clearInterval(syncInterval);
    syncInterval = null;
  }
}

async function saveEditor() {
  if (!currentEditId || !currentEditType || isSaving) return;
  
  const btn = document.getElementById('btn-save');
  const textarea = document.getElementById('editor-textarea');

  // --- INTERCEPT CONFLICT FLOW ---
  // If the background loop or server flagged a conflict, turn the commit click into a refresh hook
  if (isOutOfSync) {
    const confirmRefresh = true

    if (confirmRefresh) {
      await refreshContentsFromServer();
    }
    return;
  }
  
  const content = textarea.value;
  isSaving = true;
  btn.disabled = true;
  textarea.disabled = true;
  btn.innerText = "[ BROADCASTING TO CORE... ]";

  let res;
  
  try {
    if (currentEditType === "skills") {
        res = await fetch(`/api/${currentEditType}/${currentEditId}/notes`, {
          method: 'POST',
          body: JSON.stringify({
            text: content,
            version: currentRecordVersion
          }),
          headers: { 'Content-Type': 'application/json' }
        });
    } else {
        res = await fetch(`/api/projects/${currentEditId}/subprojects/${encodeURIComponent(subProjectName)}/notes`, {
          method: 'POST',
          body: JSON.stringify({
            text: content,
            version: currentRecordVersion
          }),
          headers: { 'Content-Type': 'application/json' }
        });
    }
    
    if (res.status === 409) {
      // Direct server conflict caught if telemetry delay happens
      isOutOfSync = true;
      triggerOutOfSyncUI();
      alert("WRITE REJECTED: Mid-flight collision detected. Core version changed. Workspace locked until aligned.");
      return;
    }

    const data = await res.json();
    currentRecordVersion = data.newVersion; // Bump local structural version index
    hasUnsavedChanges = false;
    btn.innerText = "DATA SECURED";
    
    setTimeout(() => {
      if (!isOutOfSync) btn.innerText = "COMMIT TO DATABANK";
    }, 2000);
  } catch (e) {
    btn.innerText = "ERR_WRITE_TIMEOUT";
    console.error(e);
  } finally {
    isSaving = false;
    // Only bring inputs back online if we aren't stranded out of sync
    if (!isOutOfSync) {
      btn.disabled = false;
      textarea.disabled = false;
    }
  }
}

/**
 * CORE REFRESH ROUTINE
 * Explicitly pulls fresh server text and re-aligns version matching tokens.
 */
async function refreshContentsFromServer() {
  const textarea = document.getElementById('editor-textarea');
  const btn = document.getElementById('btn-save');

  isSaving = true;
  btn.disabled = true;
  textarea.disabled = true;
  btn.innerText = "[ SYNCING LOG ENTRIES WITH CORE... ]";

  try {

    let res;

    if (currentEditType === "skills") {
          res = await fetch(`/api/${currentEditType}/${currentEditId}/notes`);
    } else {
          res = await fetch(`/api/projects/${currentEditId}/subprojects/${encodeURIComponent(subProjectName)}/notes`);
    } 

    const data = await res.json();

    textarea.value = data.text;
    currentRecordVersion = data.version; // Synchronize version track 
    isOutOfSync = false;
    hasUnsavedChanges = false;

    btn.innerText = "DATA MATRIX ALIGNED";
    btn.style.borderColor = ""; // Wipe error theme
    
    setTimeout(() => {
      btn.innerText = "COMMIT TO DATABANK";
      btn.disabled = false;
      textarea.disabled = false;
      isSaving = false;
    }, 1500);

  } catch (e) {
    btn.innerText = "ERR_SYNC_RECOVERY_FAILED";
    console.error(e);
    btn.disabled = false;
    isSaving = false;
  }
}

/**
 * REFRESH EDITOR (Visual Input Tracker)
 * Flashes unsaved modified markers if the local buffer isn't flagged out of sync.
 */
function refreshEditor() {
  hasUnsavedChanges = true;
  if (isSaving || isOutOfSync) return;
  
  const btn = document.getElementById('btn-save');
  btn.innerText = "COMMIT CHANGES* (UNSAVED DATA)";
}

/**
 * AUXILIARY UI RENDERER FOR LOCKOUT CONFLICTS
 */
function triggerOutOfSyncUI() {
  const btn = document.getElementById('btn-save');
  btn.disabled = false; // MUST stay clickable to let user issue the refresh override!
  btn.style.borderColor = "#ff0055"; // Crimson danger border layout
  btn.innerText = "[ OUT OF SYNC - CLICK TO REFRESH ]";
}

/**
 * BACKGROUND MONITOR
 * Checks the main core's version register every 3 seconds.
 */
function startBackgroundSync() {
  if (syncInterval) clearInterval(syncInterval);
  
  syncInterval = setInterval(async () => {
    if (currentEditId && currentEditType && !isSaving && !isOutOfSync) {
      try {
        let res;

        if (currentEditType === "skills") {
              res = await fetch(`/api/${currentEditType}/${currentEditId}/notes`);
        } else {
              res = await fetch(`/api/projects/${currentEditId}/subprojects/${encodeURIComponent(subProjectName)}/notes`);
        }

        const data = await res.json();
        
        // If the core version jumped ahead of our snapshot, alter UI capability
        if (data.version !== currentRecordVersion) {
          isOutOfSync = true;
          triggerOutOfSyncUI();
        }
      } catch (e) {
        console.error("Core polling stream dropped:", e);
      }
    }
  }, 3000); 
}

function previewFile(event) {
    const file = event.target.files[0];
    const reader = new FileReader();

    reader.onloadend = function() {
        const img = document.getElementById('preview-img');
        const label = document.getElementById('avatar-label');
        
        img.src = reader.result;
        img.style.display = 'block'; 
        if (label) label.style.display = 'none'; 
        
        const base64Data = reader.result; 
        document.getElementById('picture-hidden-input').value = base64Data;
    }

    if (file) {
        reader.readAsDataURL(file);
    }
}

// --- UUID ---
function generateUUID() {
    if (crypto?.randomUUID) {
        return crypto.randomUUID();
    }

    const bytes = crypto.getRandomValues(new Uint8Array(16));

    // Set version 4 (0100xxxx)
    bytes[6] = (bytes[6] & 0x0f) | 0x40;

    // Set variant (10xxxxxx)
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    const hex = [...bytes].map(b => b.toString(16).padStart(2, '0'));

    return (
        hex.slice(0, 4).join('') + '-' +
        hex.slice(4, 6).join('') + '-' +
        hex.slice(6, 8).join('') + '-' +
        hex.slice(8, 10).join('') + '-' +
        hex.slice(10, 16).join('')
    );
}

let modalRecords = [];
let currentModalIndex = 0;
let isAdding;

async function openEditModal(route) {
    const modal = document.getElementById('dynamic-edit-modal');
    const formFields = document.getElementById('form-fields');
    
    modal.classList.add('active'); 
    formFields.innerHTML = '<p style="color: var(--army-sage); text-align: center;">[ INITIALIZING RECON UPLINK... ]</p>';

    try {
        const built_route = `${route}?profile_handle=${encodeURIComponent(CURRENT_PROFILE_HANDLE)}`;
        
        const response = await fetch(built_route);
        if (!response.ok) throw new Error('Network response failed');
        
        const data = await response.json();
        
        // Save the complete array into our state variable
        modalRecords = Array.isArray(data) ? data : [data];
        currentModalIndex = 0; // Reset to the first entry
        
        // Hand off layout duties to our dedicated single-record renderer
        renderCurrentModalRecord(route);
        
    } catch (error) {
        console.error("Fetch Error:", error);
        formFields.innerHTML = '<p style="color: var(--army-red); text-align: center;">[ ERROR: CONNECTION TO NODE FAILED ]</p>';
    }
}

function renderCurrentModalRecord(route) {
    const formFields = document.getElementById('form-fields');
    const counterDisplay = document.getElementById('modal-record-counter');
    isAdding = /^\/api\/(skills|experiences|projects)\/add$/.test(route);
    
    // 1. Handle empty state (Fail-safe if no records AND we aren't adding)
    if ((!modalRecords || modalRecords.length === 0) && !isAdding) {
        formFields.innerHTML = '<p style="color: var(--army-red);">[ NO DATA RECORDS FOUND ]</p>';
        return;
    }

    // 2. Establish the profile data based on the route
    let profileData = {};
    
    if (isAdding) {
        // Create a blank template by copying keys
        const templateRecord = (modalRecords && modalRecords.length > 0) ? modalRecords[0] : { id: '', picture: '' }; 
        
        for (let key in templateRecord) {
            if (key === 'id') {
                if (route === '/api/projects/add') {
                    profileData[key] = "p" + generateUUID(); 
                } else {
                    profileData[key] = generateUUID(); 
                }
            } else {
                if (key !== 'profile_handle') {
                    profileData[key] = ''; // Blank out all other values
                } else {
                    profileData[key] = CURRENT_PROFILE_HANDLE;
                }
            }
        }
        
        if (counterDisplay) counterDisplay.innerText = `[ NEW ENTRY ]`;
    } else {
        // Pull the active record based on current tracking index
        profileData = modalRecords[currentModalIndex];
        
        if (counterDisplay) {
            const padCurrent = String(currentModalIndex + 1).padStart(2, '0');
            const padTotal = String(modalRecords.length).padStart(2, '0');
            counterDisplay.innerText = `[ ENTRY ${padCurrent} / ${padTotal} ]`;
        }
    }

    // 3. Construct the HTML strings BEFORE injecting into the DOM
    let inputsHTML = '<div class="grid-2">'; 
    let avatarHTML = '';
    
    for (const [key, value] of Object.entries(profileData)) {
        const displayValue = value !== null && value !== undefined ? value : ''; 
        
        if (key === 'picture') {
            avatarHTML = `
                <div class="avatar-upload-zone" style="grid-column: 1 / -1;">
                    <div class="avatar-frame" onclick="document.getElementById('file-input').click()">
                        <span class="avatar-label" id="avatar-label" style="display: ${displayValue ? 'none' : 'block'};">
                            [ Initialize Uplink ]<br>Select Portrait
                        </span>
                        <img id="preview-img" src="${displayValue}" style="display: ${displayValue ? 'block' : 'none'};">
                        <input type="file" id="file-input" style="display: none;" onchange="previewFile(event)">
                        <input type="hidden" name="picture" id="picture-hidden-input" value="${displayValue}">
                    </div>
                </div>
            `;
        } else {
            // ─── SECURITY CHECK FOR RESTRICTED IDENTIFIERS ───
            const isProtected = ['id', 'handle', 'profile_handle'].includes(key);
            
            inputsHTML += `
                <div class="input-group">
                    <label>${key.replace(/_/g, ' ')} ${isProtected ? '[ LOCKED ]' : ''}</label>
                    <input type="text" 
                        name="${key}" 
                        value="${displayValue}" 
                        ${isProtected ? 'disabled class="restricted-input"' : ''}>
                </div>
            `;
        }
    }
    
    inputsHTML += '</div>'; 
    
    // 4. Inject everything into the DOM at once
    formFields.innerHTML = avatarHTML + inputsHTML; 
}

function closeModal() {
    // Fade the modal out
    document.getElementById('dynamic-edit-modal').classList.remove('active');
}

// Global Event Declarations
const textarea = document.getElementById('editor-textarea');
textarea.addEventListener('input', refreshEditor);
textarea.addEventListener('paste', refreshEditor);

</script>
</body>
</html>
"##;

const FORM_HTML: &str = r##"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ADD // PROFILE</title>
    <style>
        :root {
            /* Tactical Palette */
            --army-sage: #a3b899;
            --army-sage-rgb: 163, 184, 153;
            --army-khaki: #c2b280;
            --army-khaki-rgb: 194, 178, 128;
            --army-sand: #d8cca3;
            --army-red: #b84b4b;
            --dark-bg: #131713;
            --panel-bg: rgba(163, 184, 153, 0.05);
        }

        body {
            background-color: var(--dark-bg);
            color: var(--army-sage);
            font-family: 'Courier New', Courier, monospace;
            margin: 0;
            padding: 0;
            background-image: 
                linear-gradient(rgba(var(--army-sage-rgb), 0.05) 1px, transparent 1px),
                linear-gradient(90deg, rgba(var(--army-sage-rgb), 0.05) 1px, transparent 1px);
            background-size: 20px 20px;
            height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            overflow: hidden;
        }

        /* --- MAIN DASHBOARD --- */
        .dashboard {
            text-align: center;
            border: 1px solid var(--army-sage);
            padding: 3rem;
            background: rgba(10, 13, 10, 0.85);
            box-shadow: 0 0 15px rgba(var(--army-sage-rgb), 0.1);
        }

        .dashboard h1 {
            color: var(--army-sage);
            text-shadow: 0 0 4px rgba(var(--army-sage-rgb), 0.5);
            letter-spacing: 4px;
            margin-bottom: 2rem;
            text-transform: uppercase;
        }

        .trigger-btn {
            background: transparent;
            color: var(--army-khaki);
            border: 2px solid var(--army-khaki);
            padding: 1.5rem 3rem;
            font-size: 1.2rem;
            font-family: inherit;
            font-weight: bold;
            text-transform: uppercase;
            letter-spacing: 3px;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: inset 0 0 8px rgba(var(--army-khaki-rgb), 0.2);
        }

        .trigger-btn:hover {
            background: var(--army-khaki);
            color: var(--dark-bg);
            box-shadow: 0 0 15px rgba(var(--army-khaki-rgb), 0.6);
        }

        /* --- MODAL OVERLAY --- */
        .modal-overlay {
            position: fixed;
            top: 0; left: 0; width: 100vw; height: 100vh;
            background: rgba(14, 18, 14, 0.85);
            backdrop-filter: blur(8px);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 1000;
            opacity: 0;
            pointer-events: none;
            transition: opacity 0.3s ease;
        }

        .modal-overlay.active {
            opacity: 1;
            pointer-events: auto;
        }

        .modal-content {
            background: var(--dark-bg);
            border: 1px solid var(--army-sage);
            box-shadow: 0 0 20px rgba(var(--army-sage-rgb), 0.15), inset 0 0 15px rgba(var(--army-sage-rgb), 0.05);
            width: 90%;
            max-width: 900px;
            max-height: 85vh;
            overflow-y: auto;
            padding: 2rem;
            position: relative;
            transform: translateY(20px);
            transition: transform 0.3s ease;
        }

        .modal-overlay.active .modal-content {
            transform: translateY(0);
        }

        /* Tactical Scrollbar */
        .modal-content::-webkit-scrollbar { width: 8px; }
        .modal-content::-webkit-scrollbar-track { background: rgba(var(--army-sage-rgb), 0.05); border-left: 1px solid #2d382d; }
        .modal-content::-webkit-scrollbar-thumb { background: var(--army-sage); }
        .modal-content::-webkit-scrollbar-thumb:hover { background: var(--army-khaki); }

        /* Close Button */
        .btn-close {
            position: absolute;
            top: 1rem; right: 1rem;
            background: transparent;
            color: var(--army-red);
            border: 1px solid var(--army-red);
            padding: 0.5rem 1rem;
            font-family: inherit;
            cursor: pointer;
            text-transform: uppercase;
            transition: all 0.2s ease;
        }
        .btn-close:hover {
            background: var(--army-red);
            color: var(--dark-bg);
            box-shadow: 0 0 10px rgba(184, 75, 75, 0.5);
        }

        /* --- TACTICAL AVATAR PLACEMARKER --- */
        .avatar-upload-zone {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            margin: 1.5rem 0 2rem 0;
        }

        .avatar-frame {
            width: 308px;
            height: 308px;
            border: 2px dashed var(--army-sage);
            background: rgba(var(--army-sage-rgb), 0.02);
            position: relative;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            overflow: hidden;
            transition: all 0.3s ease;
            box-shadow: 0 0 10px rgba(var(--army-sage-rgb), 0.05);
        }

        .avatar-frame:hover {
            border-color: var(--army-khaki);
            box-shadow: 0 0 15px rgba(var(--army-khaki-rgb), 0.2);
            background: rgba(var(--army-khaki-rgb), 0.04);
        }

        .avatar-frame img {
            width: 100%;
            height: 100%;
            object-fit: cover;
            display: none;
        }

        .avatar-label {
            color: var(--army-sage);
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 2px;
            text-align: center;
            padding: 1rem;
            pointer-events: none;
            transition: all 0.3s ease;
        }

        .avatar-frame:hover .avatar-label {
            color: var(--army-khaki);
            text-shadow: 0 0 4px rgba(var(--army-khaki-rgb), 0.5);
        }

        /* --- FORM STYLES --- */
        h1.modal-title { color: var(--army-khaki); text-shadow: 0 0 4px rgba(var(--army-khaki-rgb), 0.4); border-color: var(--army-khaki); margin-top: 0; text-transform: uppercase; letter-spacing: 2px; border-bottom: 1px solid var(--army-khaki); padding-bottom: 5px; }
        h2 { text-transform: uppercase; text-shadow: 0 0 4px rgba(var(--army-sage-rgb), 0.4); letter-spacing: 2px; border-bottom: 1px solid var(--army-sage); padding-bottom: 5px; margin-top: 2rem;}
        h4 { color: var(--army-sand); margin-bottom: 10px; text-transform: uppercase; border-bottom: 1px dashed #3a453a;}
        
        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; }
        .grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 1.5rem; }
        
        .input-group { display: flex; flex-direction: column; margin-bottom: 1rem; }
        label { font-size: 0.85rem; margin-bottom: 0.3rem; color: #8e998e; text-transform: uppercase; }

        input, textarea {
            background: rgba(10, 15, 10, 0.7);
            border: 1px solid #3a453a;
            color: var(--army-sage);
            padding: 0.8rem;
            font-family: inherit;
            transition: all 0.3s ease;
        }

        input:focus, textarea:focus { outline: none; border-color: var(--army-khaki); box-shadow: 0 0 8px rgba(var(--army-khaki-rgb), 0.2); }
        textarea { resize: vertical; min-height: 80px; }

        button.action-btn {
            background: transparent; color: var(--army-khaki); border: 2px solid var(--army-khaki);
            padding: 1rem 2rem; font-family: inherit; font-weight: bold; text-transform: uppercase;
            letter-spacing: 2px; cursor: pointer; width: 100%; margin-top: 2rem; transition: all 0.2s ease;
        }
        button.action-btn:hover { background: var(--army-khaki); color: var(--dark-bg); box-shadow: 0 0 15px rgba(var(--army-khaki-rgb), 0.5); }

        .btn-add { border: 1px solid var(--army-sage); background: transparent; color: var(--army-sage); padding: 0.5rem 1rem; margin-top: 0; cursor: pointer; text-transform: uppercase; font-family: inherit; transition: all 0.2s ease;}
        .btn-add:hover { background: var(--army-sage); color: var(--dark-bg); box-shadow: 0 0 10px rgba(var(--army-sage-rgb), 0.5); }

        .btn-remove { border: 1px solid var(--army-red); background: transparent; color: var(--army-red); padding: 0.4rem; font-size: 0.8rem; width: auto; margin-top: 0; cursor: pointer; text-transform: uppercase; font-family: inherit; transition: all 0.2s ease;}
        .btn-remove:hover { background: var(--army-red); color: var(--dark-bg); box-shadow: 0 0 10px rgba(184, 75, 75, 0.5); }

        .dynamic-entry { border: 1px dashed #3a453a; padding: 1rem; margin-bottom: 1rem; background: rgba(20, 26, 20, 0.4); }
        .section-header { display: flex; justify-content: space-between; align-items: baseline; }
        .array-hint { font-size: 0.7rem; color: #6d7a6d; margin-top: 4px; }
</style>
</head>
<body>

    <div class="dashboard">
        <h1>ADD // PROFILE</h1>
        <p style="margin-bottom: 2rem;">SYSTEM STATUS: SECURE</p>
        <button class="trigger-btn" onclick="openUplink()">> START</button>
    </div>

    <div id="uplink-modal" class="modal-overlay" onclick="closeOnBackgroundClick(event)">
        <div class="modal-content">
            <button class="btn-close" onclick="closeUplink()">[ X ] Abort</button>
            
            <h1 class="modal-title">PROFILE // SUBMISSION FORM</h1>

            <form id="uplink-form">
                
                <div class="avatar-upload-zone">
                    <label style="margin-bottom: 0.5rem;">[ PICTURE ]</label>
                    <div class="avatar-frame" id="avatar-click-zone" onclick="triggerFileSearch()">
                        <div class="avatar-label" id="avatar-text-status">// Click to mount core profile image (.JPG)</div>
                        <img id="avatar-render-target" alt="Neural Interface Matrix Identity Construct">
                    </div>
                    <input type="file" id="identity-picture-input" accept=".jpg, .jpeg" style="display: none;" onchange="validateAndDisplayPicture(event)">
                </div>

                <h2>[01] PROFILE // IDENTITY (Static)</h2>
                <div class="grid-2">
                    <div class="input-group"><label>Handle</label><input type="text" id="handle" placeholder="@netrunner_99"></div>
                    <div class="input-group"><label>Real Name</label><input type="text" id="name" placeholder="Case"></div>
                    <div class="input-group"><label>Title</label><input type="text" id="title" placeholder="Systems Architect"></div>
                    <div class="input-group"><label>Location</label><input type="text" id="location" placeholder="Chiba City"></div>
                </div>
                <div class="input-group"><label>Summary</label><textarea id="summary" placeholder="Enter high-level directive..."></textarea></div>

                <h2>[02] SELF-EVALUATION (Static)</h2>
                <div class="grid-3">
                    <div class="input-group"><label>Leadership (0-100)</label><input type="number" id="leadership" min="0" max="100"></div>
                    <div class="input-group"><label>Tech Depth (0-100)</label><input type="number" id="technical_depth" min="0" max="100"></div>
                    <div class="input-group"><label>Automation (0-100)</label><input type="number" id="automation_index" min="0" max="100"></div>
                    <div class="input-group"><label>Transferability (0-100)</label><input type="number" id="transferability" min="0" max="100"></div>
                    <div class="input-group"><label>Innovation (0-100)</label><input type="number" id="innovation" min="0" max="100"></div>
                    <div class="input-group"><label>Neural Load (0-100)</label><input type="number" id="neural_load" min="0" max="100"></div>
                </div>

                <div class="section-header">
                    <h2>[03] TRANSFERABLE SKILLS (Dynamic)</h2>
                    <button type="button" class="btn-add" onclick="addNode('skills-container', generateSkillHTML)">+ Add Skill</button>
                </div>
                <div id="skills-container"></div>

                <div class="section-header">
                    <h2>[04] EXPERIENCES (Dynamic)</h2>
                    <button type="button" class="btn-add" onclick="addNode('experiences-container', generateExperienceHTML)">+ Add Experience</button>
                </div>
                <div id="experiences-container"></div>

                <div class="section-header">
                    <h2>[05] PROJECTS (Dynamic)</h2>
                    <button type="button" class="btn-add" onclick="addNode('projects-container', generateProjectHTML)">+ Add Project</button>
                </div>
                <div id="projects-container"></div>

                <button type="button" class="action-btn" onclick="executeUplink()">Submit Profile >_</button>
            </form>
        </div>
    </div>

<script>
    // Global Payload Storage Node Variable
    let profilePictureBase64 = "";

    // --- MODAL LOGIC ---
    const modal = document.getElementById('uplink-modal');

    function openUplink() {
        modal.classList.add('active');
    }

    function closeUplink() {
        modal.classList.remove('active');
    }

    function closeOnBackgroundClick(event) {
        if (event.target === modal) {
            closeUplink();
        }
    }

    document.addEventListener('keydown', function(event) {
        if (event.key === "Escape" && modal.classList.contains('active')) {
            closeUplink();
        }
    });

    // --- UUID ---
   function generateUUID() {
        if (crypto?.randomUUID) {
            return crypto.randomUUID();
        }

        const bytes = crypto.getRandomValues(new Uint8Array(16));

        // Set version 4 (0100xxxx)
        bytes[6] = (bytes[6] & 0x0f) | 0x40;

        // Set variant (10xxxxxx)
        bytes[8] = (bytes[8] & 0x3f) | 0x80;

        const hex = [...bytes].map(b => b.toString(16).padStart(2, '0'));

        return (
            hex.slice(0, 4).join('') + '-' +
            hex.slice(4, 6).join('') + '-' +
            hex.slice(6, 8).join('') + '-' +
            hex.slice(8, 10).join('') + '-' +
            hex.slice(10, 16).join('')
        );
    }

    // --- BASE64 ASYNC CONVERSION SUBSYSTEM ---
    function convertFileToBase64(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.readAsDataURL(file);
            reader.onload = () => resolve(reader.result);
            reader.onerror = (error) => reject(error);
        });
    }

    // --- FILE EXPLORER DIALOG & EXTENSION SIGNATURE VALIDATION ---
    function triggerFileSearch() {
        document.getElementById('identity-picture-input').click();
    }

    async function validateAndDisplayPicture(event) {
        const file = event.target.files[0];
        if (!file) return;

        // Verify Extension Syntax Rules
        const fileName = file.name.toLowerCase();
        if (!fileName.endsWith('.jpg') && !fileName.endsWith('.jpeg')) {
            alert("CRITICAL UPLINK ERROR // INVALID FILE EXTENSION. CORE ARCHITECTURE DEMANDS .JPG SYNTAX.");
            event.target.value = ""; 
            return;
        }

        try {
            // Await the pipeline conversion promise asynchronously
            const base64Data = await convertFileToBase64(file);
            
            // Map the resolved array string to the memory tracking target
            profilePictureBase64 = base64Data;

            // Render output matrix visualizer immediately
            const displayImg = document.getElementById('avatar-render-target');
            const statusText = document.getElementById('avatar-text-status');
            
            displayImg.src = base64Data;
            displayImg.style.display = 'block';
            statusText.style.display = 'none';

        } catch (err) {
            console.error("Matrix conversion loop failure:", err);
            alert("FATAL // FAILED TO PARSE PICTURE INTO NEURAL ARRAY STREAM.");
        }
    }

    // --- UI INJECTION ROUTINES ---
    function addNode(containerId, generatorFunc) {
        const container = document.getElementById(containerId);
        container.insertAdjacentHTML('beforeend', generatorFunc());
    }

    function generateSkillHTML() {
        return `
        <div class="dynamic-entry skill-entry">
            <h4>Skill Node</h4>
            <div class="grid-2">
                <div class="input-group"><label>Name</label><input type="text" class="s-name" placeholder="Rust"></div>
                <div class="input-group"><label>Category</label><input type="text" class="s-cat" placeholder="Backend"></div>
                <div class="input-group"><label>Score (0-100)</label><input type="number" class="s-score" placeholder="90"></div>
                <div class="input-group">
                    <label>Links</label>
                    <input type="text" class="s-links" placeholder="Python">
                    <span class="array-hint">Comma separated skills nodes links</span>
                </div>
            </div>
            <button type="button" class="btn-remove" onclick="this.parentElement.remove()">- Terminate Node</button>
        </div>`;
    }

    function generateExperienceHTML() {
        return `
        <div class="dynamic-entry exp-entry">
            <h4>Experience Log</h4>
            <div class="grid-2">
                <div class="input-group"><label>Role</label><input type="text" class="e-role" placeholder="Lead Netrunner"></div>
                <div class="input-group"><label>Megacorp / Org</label><input type="text" class="e-org" placeholder="Tyrell Corp"></div>
                <div class="input-group"><label>Years Active</label><input type="number" step="0.5" class="e-years" placeholder="4.5"></div>
                <div class="input-group"><label>Summary</label><input type="text" class="e-summary" placeholder="Brief overview..."></div>
            </div>
            <div class="grid-2">
                <div class="input-group">
                    <label>Achievements</label>
                    <textarea class="e-achievements" placeholder="Bypassed ICE, Secured payload..."></textarea>
                    <span class="array-hint">Comma separated values</span>
                </div>
                <div class="input-group">
                    <label>Tech Skills Applied</label>
                    <textarea class="e-skills" placeholder="Rust, WASM, SQLx"></textarea>
                    <span class="array-hint">Comma separated values</span>
                </div>
            </div>
            <button type="button" class="btn-remove" onclick="this.parentElement.remove()">- Terminate Node</button>
        </div>`;
    }

    function generateProjectHTML() {
        return `
        <div class="dynamic-entry proj-entry">
            <h4>Project Archive</h4>
            <div class="grid-2">
                <div class="input-group"><label>Project Name</label><input type="text" class="p-name" placeholder="Project WINTERMUTE"></div>
                <div class="input-group"><label>Impact (0-100)</label><input type="number" class="p-impact" placeholder="99"></div>
            </div>
            <div class="input-group"><label>Description</label><textarea class="p-desc" placeholder="Details of the construct..."></textarea></div>
            <div class="input-group">
                <label>Technologies Used</label>
                <input type="text" class="p-tech" placeholder="AI, Blockchain, Rust">
                <span class="array-hint">Comma separated values</span>
            </div>
            <button type="button" class="btn-remove" onclick="this.parentElement.remove()">- Terminate Node</button>
        </div>`;
    }

    // --- PAYLOAD COMPILATION ROUTINES ---
    const parseArray = (str) => str ? str.split(',').map(s => s.trim()).filter(s => s) : [];

    function executeUplink() {
        const skills = Array.from(document.querySelectorAll('.skill-entry')).map(node => ({
            id: generateUUID(),
            profile_handle: document.getElementById('handle').value,
            name: node.querySelector('.s-name').value,
            category: node.querySelector('.s-cat').value,
            score: parseInt(node.querySelector('.s-score').value || 0),
            links: parseArray(node.querySelector('.s-links').value)
        }));

        const experiences = Array.from(document.querySelectorAll('.exp-entry')).map(node => ({
            id: generateUUID(),
            profile_handle: document.getElementById('handle').value,
            role: node.querySelector('.e-role').value,
            organization: node.querySelector('.e-org').value,
            years: parseFloat(node.querySelector('.e-years').value || 0.0),
            summary: node.querySelector('.e-summary').value,
            achievements: parseArray(node.querySelector('.e-achievements').value),
            skills: parseArray(node.querySelector('.e-skills').value)
        }));

        const projects = Array.from(document.querySelectorAll('.proj-entry')).map(node => ({
            id: "p" + generateUUID(),
            profile_handle: document.getElementById('handle').value,
            name: node.querySelector('.p-name').value,
            impact: parseInt(node.querySelector('.p-impact').value || 0),
            description: node.querySelector('.p-desc').value,
            technologies: parseArray(node.querySelector('.p-tech').value)
        }));

        const payload = {
            profile: {
                handle: document.getElementById('handle').value,
                name: document.getElementById('name').value,
                title: document.getElementById('title').value,
                location: document.getElementById('location').value,
                summary: document.getElementById('summary').value,
                picture: profilePictureBase64 // Dispatched as an inline base64 string variable
            },
            analytics: {
                id: document.getElementById('handle').value,
                leadership: parseInt(document.getElementById('leadership').value || 0),
                technical_depth: parseInt(document.getElementById('technical_depth').value || 0),
                automation_index: parseInt(document.getElementById('automation_index').value || 0),
                transferability: parseInt(document.getElementById('transferability').value || 0),
                innovation: parseInt(document.getElementById('innovation').value || 0),
                neural_load: parseInt(document.getElementById('neural_load').value || 0),
            },
            skills: skills,
            experiences: experiences,
            projects: projects
        };

        fetch('/api/downlink', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        })
        .then(response => {
            if (response.ok) {
                alert("TRANSMISSION SUCCESSFUL // DATA WRITTEN TO CORE.");
                closeUplink();
            } else {
                alert("TRANSMISSION FAILED // SERVER REJECTED PAYLOAD.");
            }
        })
        .catch(error => {
            console.error("Uplink Error:", error);
            alert("CRITICAL ERROR // CONNECTION SEVERED.");
        });

        const handleValue = payload.profile.handle + " profile was added";

        // NEW USER HEADLINE
        fetch('/api/push-news', {
            method: 'POST',
            headers: {
                'Content-Type': 'text/plain'
            },
            body: handleValue
        });
    }

    // Initialize with one of each dynamic node
    window.onload = () => {
        addNode('skills-container', generateSkillHTML);
        addNode('experiences-container', generateExperienceHTML);
        addNode('projects-container', generateProjectHTML);
    };
</script>

</body>
</html>
"##;