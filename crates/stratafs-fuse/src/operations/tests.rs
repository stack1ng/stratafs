use super::*;

#[test]
fn a_name_in_the_user_namespace_is_portable_without_the_namespace() {
    assert_eq!(
        annotation_key(b"user.mime_type"),
        AnnotationKey::Portable(b"mime_type")
    );
}

#[test]
fn a_name_in_any_other_namespace_is_linux_only() {
    for name in [
        b"security.selinux".as_slice(),
        b"trusted.overlay",
        b"com.apple.quarantine",
    ] {
        assert_eq!(
            annotation_key(name),
            AnnotationKey::Platform {
                platform: LINUX,
                name
            }
        );
    }
}

#[test]
fn listed_names_come_back_as_they_were_set() {
    let names = xattr_names(
        &[Bytes::from_static(b"mime_type")],
        &[Bytes::from_static(b"trusted.overlay")],
    );
    assert_eq!(names, b"user.mime_type\0trusted.overlay\0");
    for name in names
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
    {
        let listed = match annotation_key(name) {
            AnnotationKey::Portable(name) => xattr_names(&[Bytes::copy_from_slice(name)], &[]),
            AnnotationKey::Platform { name, .. } => {
                xattr_names(&[], &[Bytes::copy_from_slice(name)])
            }
        };
        assert_eq!(&listed[..listed.len() - 1], name);
    }
}

#[test]
fn a_listing_from_the_start_begins_with_dot_and_dot_dot() {
    let dots: Vec<_> = dots(Inode(7), Inode(3), 0).collect();
    assert_eq!(
        dots,
        [
            (Inode(7), 1, b".".as_slice()),
            (Inode(3), 2, b"..".as_slice())
        ]
    );
}

#[test]
fn a_listing_carried_on_past_the_dots_lists_them_no_more() {
    assert_eq!(dots(Inode(7), Inode(3), 1).count(), 1);
    assert_eq!(dots(Inode(7), Inode(3), 2).count(), 0);
    assert_eq!(dots(Inode(7), Inode(3), 1 << 20).count(), 0);
}
