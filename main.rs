use axum::{
    extract::{Path, State, Query},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json,
    Router,
    response::sse::{Event, Sse},
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
    let file_path = format!("./project_notes/{}_{}.txt", id, subproject_name); //tmp
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
    let file_path = format!("./project_notes/{}_{}.txt", id, subproject_name); //tmp
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
    let file_path = format!("./skill_notes/{}.txt", id); //tmp
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
    let file_path = format!("./skill_notes/{}.txt", id); //tmp
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
        .filename("./Resume_profiles.db") // Looks in the current directory //tmp
        .create_if_missing(true);       // The magic flag!

    // 2. Build the pool using those options
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await?;

    init_db(&pool).await?;
    tracing::info!("Database schema initialized successfully.");

    tracing_subscriber::fmt::init();

    // Initialize databank sectors
    if let Err(e) = tokio::fs::create_dir_all("./project_notes").await { //tmp
        tracing::error!("Failed to initialize project vault: {}", e);
    }
    // -- NEW: Secure local storage sector for skills --
    if let Err(e) = tokio::fs::create_dir_all("./skill_notes").await { //tmp
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
        .layer(CompressionLayer::new())
        .with_state(state);

    let addr = SocketAddr::from(([127,0,0,1], 3000)); //or proxy_pass [127,0,0,1], 3000 with nginx.

    tracing::info!("ARES MAINFRAME ONLINE");
    tracing::info!("Listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.expect("failed to bind listener");
    if let Err(err) = axum::serve(listener, app).await {
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
          name: "Jordan 'CRUSADER' Legare".into(),
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
        let group_id = group.first()?.id.clone();
        
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
  overflow: hidden;
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
  position: relative; z-index: 10; height: 100vh; padding: 20px;
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
    <button class="uplink-btn" onclick="initUplink()">INITIALIZE UPLINK</button>
    <button id="openModalBtn" class="uplink-btn hidden">INITIATE OVERRIDE</button>
  </header>

  <section class="panel" style="grid-column: 1; grid-row: 2;">
    <div class="panel-title">
    <span>SUBJECT_INTEL</span>
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
        <span>MATRIX_SKILLS</span>
        
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
        <span>CHRONOS_LOGS</span>
        
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
        <span>NEURAL_PROJECTS</span>
        
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
    <div class="panel-title">NODE_INSPECTOR</div>
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
  
  const prefix = type === 'skills' ? 'MATRIX_SKILL' : 'NEURAL_PROJ';
  document.getElementById('editor-title-text').innerText = `${prefix} // ${name} // INTEL_LOG`;
  document.getElementById('editor-modal').style.display = 'flex';
  
  const textarea = document.getElementById('editor-textarea');
  textarea.value = "[ RETRIEVING MEMORY BLOCK... ]";
  textarea.disabled = true;

  const btn = document.getElementById('btn-save');
  btn.disabled = false;
  btn.style.borderColor = ""; // Reset custom styles
  btn.innerText = "COMMIT TO DATABANK";

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
    <title>NEURAL UPLINK // HYBRID_CONSTRUCT</title>
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
        <h1>Sys_Admin // Core</h1>
        <p style="margin-bottom: 2rem;">SYSTEM STATUS: SECURE</p>
        <button class="trigger-btn" onclick="openUplink()">> Initiate Uplink</button>
    </div>

    <div id="uplink-modal" class="modal-overlay" onclick="closeOnBackgroundClick(event)">
        <div class="modal-content">
            <button class="btn-close" onclick="closeUplink()">[ X ] Abort</button>
            
            <h1 class="modal-title">System_Override // Hybrid_Upload</h1>

            <form id="uplink-form">
                
                <div class="avatar-upload-zone">
                    <label style="margin-bottom: 0.5rem;">[ VISUAL_CONSTRUCT_MATRIX ]</label>
                    <div class="avatar-frame" id="avatar-click-zone" onclick="triggerFileSearch()">
                        <div class="avatar-label" id="avatar-text-status">// Click to mount core profile image (.JPG)</div>
                        <img id="avatar-render-target" alt="Neural Interface Matrix Identity Construct">
                    </div>
                    <input type="file" id="identity-picture-input" accept=".jpg, .jpeg" style="display: none;" onchange="validateAndDisplayPicture(event)">
                </div>

                <h2>[01] Identity Matrix (Static)</h2>
                <div class="grid-2">
                    <div class="input-group"><label>Handle</label><input type="text" id="handle" placeholder="@netrunner_99"></div>
                    <div class="input-group"><label>Real Name</label><input type="text" id="name" placeholder="Case"></div>
                    <div class="input-group"><label>Title</label><input type="text" id="title" placeholder="Systems Architect"></div>
                    <div class="input-group"><label>Location</label><input type="text" id="location" placeholder="Chiba City"></div>
                </div>
                <div class="input-group"><label>Summary</label><textarea id="summary" placeholder="Enter high-level directive..."></textarea></div>

                <h2>[02] Neural Diagnostics (Static)</h2>
                <div class="grid-3">
                    <div class="input-group"><label>Leadership (0-100)</label><input type="number" id="leadership" min="0" max="100"></div>
                    <div class="input-group"><label>Tech Depth (0-100)</label><input type="number" id="technical_depth" min="0" max="100"></div>
                    <div class="input-group"><label>Automation (0-100)</label><input type="number" id="automation_index" min="0" max="100"></div>
                    <div class="input-group"><label>Transferability (0-100)</label><input type="number" id="transferability" min="0" max="100"></div>
                    <div class="input-group"><label>Innovation (0-100)</label><input type="number" id="innovation" min="0" max="100"></div>
                    <div class="input-group"><label>Neural Load (0-100)</label><input type="number" id="neural_load" min="0" max="100"></div>
                </div>

                <div class="section-header">
                    <h2>[03] Skill Subroutines (Dynamic)</h2>
                    <button type="button" class="btn-add" onclick="addNode('skills-container', generateSkillHTML)">+ Inject Skill</button>
                </div>
                <div id="skills-container"></div>

                <div class="section-header">
                    <h2>[04] Experience Logs (Dynamic)</h2>
                    <button type="button" class="btn-add" onclick="addNode('experiences-container', generateExperienceHTML)">+ Inject Log</button>
                </div>
                <div id="experiences-container"></div>

                <div class="section-header">
                    <h2>[05] Project Archives (Dynamic)</h2>
                    <button type="button" class="btn-add" onclick="addNode('projects-container', generateProjectHTML)">+ Inject Project</button>
                </div>
                <div id="projects-container"></div>

                <button type="button" class="action-btn" onclick="executeUplink()">Transmit Hybrid Payload >_</button>
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
                id: generateUUID(),
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