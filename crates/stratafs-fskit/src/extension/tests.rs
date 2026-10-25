use std::io;
use std::sync::{Arc, Mutex};

use block2::RcBlock;
use objc2::{AllocAnyThread, DefinedClass, msg_send};
use objc2_foundation::{NSError, NSString, NSURL};
use objc2_fs_kit::{FSGenericURLResource, FSTaskOptions, FSUnaryFileSystemOperations, FSVolume};

use super::*;

// Replied is what loadResource passed to its reply: the opened volume id, or the error's code and description.
type Replied = Option<Result<Bytes, (isize, String)>>;

fn load(file_system: &StrataFileSystem, url: &str) -> Replied {
    let replied: Arc<Mutex<Replied>> = Arc::default();
    let reply: RcBlock<dyn Fn(*mut FSVolume, *mut NSError)> = RcBlock::new({
        let replied = replied.clone();
        move |volume: *mut FSVolume, error: *mut NSError| {
            // SAFETY: FSKit's reply passes either a live volume or a live error.
            let result = unsafe {
                match (volume.cast::<StrataVolume>().as_ref(), error.as_ref()) {
                    (Some(volume), None) => Ok(volume
                        .ivars()
                        .downcast_ref::<Bytes>()
                        .expect("the test opens volumes as their id")
                        .clone()),
                    (None, Some(error)) => {
                        Err((error.code(), error.localizedDescription().to_string()))
                    }
                    _ => panic!("the reply passes exactly one of a volume and an error"),
                }
            };
            *replied.lock().unwrap() = Some(result);
        }
    });
    // SAFETY: the resource, options and reply are all valid for the call.
    unsafe {
        let url = NSURL::URLWithString(&NSString::from_str(url)).expect("the test URL is valid");
        let resource = FSGenericURLResource::initWithURL(FSGenericURLResource::alloc(), &url);
        file_system.loadResource_options_replyHandler(&resource, &FSTaskOptions::new(), &reply);
    }
    replied.lock().unwrap().take()
}

#[test]
fn load_resource_opens_the_volume_its_url_names_or_replies_why_not() {
    OPEN_VOLUME
        .set(opener(async |volume: Bytes| match &volume[..] {
            [0xff] => Err(io::Error::other("the store is down")),
            _ => Ok::<_, io::Error>(volume),
        }))
        .unwrap_or_else(|_| panic!("only this test installs an opener"));
    // SAFETY: init is NSObject's initializer, which FSUnaryFileSystem inherits.
    let file_system: Retained<StrataFileSystem> =
        unsafe { msg_send![StrataFileSystem::alloc(), init] };

    assert_eq!(
        load(&file_system, "stratavolume://ab01"),
        Some(Ok(Bytes::from_static(&[0xab, 0x01])))
    );
    assert!(matches!(
        load(&file_system, "https://ab01"),
        Some(Err((code, description)))
            if code == libc::EINVAL as isize && description.contains("does not name a volume")
    ));
    assert!(matches!(
        load(&file_system, "stratavolume://ff"),
        Some(Err((code, description)))
            if code == libc::EIO as isize && description.contains("the store is down")
    ));
}
