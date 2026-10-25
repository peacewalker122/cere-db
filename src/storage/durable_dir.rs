use std::{
    collections::VecDeque,
    path::{Component, Path, PathBuf},
};

use crate::error::DBError;

#[cfg(test)]
static FAIL_SYNC_ONCE: std::sync::Mutex<Option<std::path::PathBuf>> = std::sync::Mutex::new(None);
#[cfg(test)]
static SYNCED_DIRECTORIES: std::sync::Mutex<Vec<std::path::PathBuf>> =
    std::sync::Mutex::new(Vec::new());
#[cfg(test)]
static TEST_HOOK_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(crate) async fn create_dir_all_durable(path: &Path) -> Result<(), DBError> {
    if let Ok(metadata) = tokio::fs::metadata(path).await {
        if !metadata.is_dir() {
            return Err(DBError::StorageError(format!(
                "not a directory: {}",
                path.display()
            )));
        }
    }
    tokio::fs::create_dir_all(path).await?;

    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    for directory in resolved_sync_chain(&absolute_path).await? {
        sync_directory(&directory).await?;
    }
    Ok(())
}

async fn resolved_sync_chain(path: &Path) -> Result<Vec<PathBuf>, DBError> {
    let mut pending = path
        .components()
        .filter_map(|component| match component {
            Component::Prefix(prefix) => Some(prefix.as_os_str().to_os_string()),
            Component::RootDir => None,
            Component::CurDir => Some(".".into()),
            Component::ParentDir => Some("..".into()),
            Component::Normal(name) => Some(name.to_os_string()),
        })
        .collect::<VecDeque<_>>();
    let mut resolved = if path.is_absolute() {
        PathBuf::from(std::path::MAIN_SEPARATOR.to_string())
    } else {
        std::env::current_dir()?
    };
    let mut traversed_directories = vec![resolved.clone()];
    let mut link_parents = Vec::new();
    let mut hops = 0;

    while let Some(component) = pending.pop_front() {
        if component == "." {
            continue;
        }
        if component == ".." {
            traversed_directories.push(resolved.clone());
            resolved.pop();
            continue;
        }
        let candidate = resolved.join(&component);
        let metadata = tokio::fs::symlink_metadata(&candidate).await?;
        if metadata.file_type().is_symlink() {
            hops += 1;
            if hops > 40 {
                return Err(std::io::Error::other("too many symbolic links").into());
            }
            link_parents.push(resolved.clone());
            let target = tokio::fs::read_link(&candidate).await?;
            let mut target_components = target
                .components()
                .filter_map(|component| match component {
                    Component::Prefix(prefix) => Some(prefix.as_os_str().to_os_string()),
                    Component::RootDir => None,
                    Component::CurDir => Some(".".into()),
                    Component::ParentDir => Some("..".into()),
                    Component::Normal(name) => Some(name.to_os_string()),
                })
                .collect::<VecDeque<_>>();
            if target.is_absolute() {
                resolved = PathBuf::from(std::path::MAIN_SEPARATOR.to_string());
                traversed_directories.push(resolved.clone());
            }
            target_components.append(&mut pending);
            pending = target_components;
        } else {
            resolved = candidate;
            traversed_directories.push(resolved.clone());
        }
    }

    let mut chain = Vec::new();
    for parent in std::iter::once(resolved)
        .chain(traversed_directories)
        .chain(link_parents)
    {
        let mut directory = Some(parent);
        while let Some(path) = directory {
            let canonical = tokio::fs::canonicalize(&path).await?;
            if !chain.contains(&canonical) {
                chain.push(canonical);
            }
            directory = path.parent().map(Path::to_path_buf);
        }
    }
    Ok(chain)
}

async fn sync_directory(path: &Path) -> Result<(), DBError> {
    #[cfg(test)]
    if let Ok(mut failure) = FAIL_SYNC_ONCE.lock() {
        if failure.as_deref() == Some(path) {
            *failure = None;
            return Err(std::io::Error::from(std::io::ErrorKind::Other).into());
        }
    }
    tokio::fs::File::open(path).await?.sync_all().await?;
    #[cfg(test)]
    SYNCED_DIRECTORIES
        .lock()
        .expect("sync trace mutex poisoned")
        .push(path.to_path_buf());
    Ok(())
}

pub(crate) async fn sync_parent(path: &Path) -> Result<(), DBError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .or_else(|| Some(Path::new(".")))
    {
        tokio::fs::File::open(parent).await?.sync_all().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{FAIL_SYNC_ONCE, SYNCED_DIRECTORIES, TEST_HOOK_LOCK, create_dir_all_durable};

    #[tokio::test]
    async fn parent_traversal_detours_are_synced_and_retry_the_complete_chain() {
        let _hook_guard = TEST_HOOK_LOCK.lock().await;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("transit/deep/../../R/retry/a/b/db");
        let detour_parent = root.path().join("transit");
        SYNCED_DIRECTORIES.lock().unwrap().clear();
        *FAIL_SYNC_ONCE.lock().unwrap() = Some(detour_parent.clone());
        assert!(create_dir_all_durable(&path).await.is_err());

        SYNCED_DIRECTORIES.lock().unwrap().clear();
        create_dir_all_durable(&path).await.unwrap();
        let synced = SYNCED_DIRECTORIES.lock().unwrap().clone();
        assert!(synced.contains(&detour_parent.canonicalize().unwrap()));
        assert!(synced.contains(&root.path().join("transit/deep").canonicalize().unwrap()));
        assert!(synced.contains(&root.path().join("R/retry/a/b").canonicalize().unwrap()));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let alias_parent = root.path().join("alias");
            std::fs::create_dir_all(&alias_parent).unwrap();
            let alias = alias_parent.join("db");
            symlink("../transit/deep/../../R/retry/a/b/db", &alias).unwrap();
            SYNCED_DIRECTORIES.lock().unwrap().clear();
            create_dir_all_durable(&alias).await.unwrap();
            let synced = SYNCED_DIRECTORIES.lock().unwrap().clone();
            assert!(synced.contains(&detour_parent.canonicalize().unwrap()));
            assert!(synced.contains(&root.path().join("transit/deep").canonicalize().unwrap()));
            assert!(synced.contains(&alias_parent.canonicalize().unwrap()));
        }
    }

    #[tokio::test]
    async fn retry_syncs_existing_ancestors_after_initial_failure() {
        let _hook_guard = TEST_HOOK_LOCK.lock().await;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("retry/a/b/db");
        let failed_parent = root.path().join("retry/a");
        *FAIL_SYNC_ONCE.lock().unwrap() = Some(failed_parent.clone());
        SYNCED_DIRECTORIES.lock().unwrap().clear();
        assert!(create_dir_all_durable(&path).await.is_err());

        SYNCED_DIRECTORIES.lock().unwrap().clear();
        create_dir_all_durable(&path).await.unwrap();

        let synced = SYNCED_DIRECTORIES.lock().unwrap().clone();
        assert_eq!(synced[0], path);
        assert_eq!(synced[1], root.path().join("retry/a/b"));
        assert_eq!(synced[2], failed_parent);
        assert_eq!(synced[3], root.path().join("retry"));
        assert_eq!(synced[4], root.path());
    }

    #[tokio::test]
    async fn relative_and_symlink_paths_sync_resolved_ancestry() {
        let _hook_guard = TEST_HOOK_LOCK.lock().await;
        let root = tempfile::Builder::new().tempdir_in(".").unwrap();
        let relative = root.path().join("relative/a/b");
        let failed_parent = root
            .path()
            .join("relative/a")
            .canonicalize()
            .unwrap_or_else(|_| {
                std::fs::create_dir_all(root.path().join("relative/a")).unwrap();
                root.path().join("relative/a").canonicalize().unwrap()
            });
        SYNCED_DIRECTORIES.lock().unwrap().clear();
        *FAIL_SYNC_ONCE.lock().unwrap() = Some(failed_parent);
        assert!(create_dir_all_durable(&relative).await.is_err());
        SYNCED_DIRECTORIES.lock().unwrap().clear();
        create_dir_all_durable(&relative).await.unwrap();
        let synced = SYNCED_DIRECTORIES.lock().unwrap().clone();
        assert!(synced.contains(&relative.canonicalize().unwrap()));
        assert!(synced.contains(&root.path().canonicalize().unwrap()));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let target = root.path().join("target/deep/database");
            std::fs::create_dir_all(&target).unwrap();
            let alias_parent = root.path().join("alias-parent");
            std::fs::create_dir_all(&alias_parent).unwrap();
            let alias = alias_parent.join("db-link");
            symlink(root.path().join("target"), &alias).unwrap();
            let alias_database = alias.join("deep/database");
            SYNCED_DIRECTORIES.lock().unwrap().clear();
            create_dir_all_durable(&alias_database).await.unwrap();
            let synced = SYNCED_DIRECTORIES.lock().unwrap().clone();
            assert!(synced.contains(&target.canonicalize().unwrap()));
            assert!(synced.contains(&alias_parent.canonicalize().unwrap()));
            assert!(synced.contains(&root.path().join("target").canonicalize().unwrap()));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn nested_symlink_targets_and_dotdot_sync_every_alias_ancestry() {
        let _hook_guard = TEST_HOOK_LOCK.lock().await;
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let middle = root.path().join("middle/nested/deep");
        std::fs::create_dir_all(&middle).unwrap();
        let target = root.path().join("R/retry/a/b");
        std::fs::create_dir_all(&target).unwrap();
        let first = root.path().join("outer/deep/entry");
        std::fs::create_dir_all(first.parent().unwrap()).unwrap();
        symlink("../../middle/nested/deep/hop", &first).unwrap();
        symlink("../../../R/retry/a/b", middle.join("hop")).unwrap();

        let database = first.join("db");
        *FAIL_SYNC_ONCE.lock().unwrap() = Some(middle.clone());
        SYNCED_DIRECTORIES.lock().unwrap().clear();
        assert!(create_dir_all_durable(&database).await.is_err());

        SYNCED_DIRECTORIES.lock().unwrap().clear();
        create_dir_all_durable(&database).await.unwrap();
        let synced = SYNCED_DIRECTORIES.lock().unwrap().clone();
        for required in [
            target.clone(),
            root.path().join("R/retry/a"),
            root.path().join("R/retry"),
            middle.clone(),
            root.path().join("middle/nested"),
            root.path().join("middle"),
            root.path().join("outer/deep"),
            root.path().join("outer"),
        ] {
            assert!(
                synced.contains(&required.canonicalize().unwrap()),
                "missing {}",
                required.display()
            );
        }
        assert_eq!(synced[0], database.canonicalize().unwrap());

        let dotdot_database = root.path().join("outer/deep/entry/../b/db");
        SYNCED_DIRECTORIES.lock().unwrap().clear();
        create_dir_all_durable(&dotdot_database).await.unwrap();
        let synced = SYNCED_DIRECTORIES.lock().unwrap().clone();
        assert!(synced.contains(&target.canonicalize().unwrap()));
        assert!(synced.contains(&middle.canonicalize().unwrap()));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_loop_fails_closed() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first");
        let second = root.path().join("second");
        symlink("second", &first).unwrap();
        symlink("first", &second).unwrap();
        assert!(create_dir_all_durable(&first.join("db")).await.is_err());
    }
}
