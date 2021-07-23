//! `feature = "trace"` Tracing and profiling functions. Error and warning log.

use std::ffi::{CStr, CString};
use std::mem;
use std::os::raw::{c_char, c_int, /*c_uint, */c_void};
use std::panic::catch_unwind;
use std::ptr;
use std::sync::{Arc, Weak};
use std::time::Duration;

use super::ffi;
use crate::error::error_from_sqlite_code;
use crate::{Connection, Result};

/// `feature = "trace"` Set up the process-wide SQLite error logging callback.
///
/// # Safety
///
/// This function is marked unsafe for two reasons:
///
/// * The function is not threadsafe. No other SQLite calls may be made while
///   `config_log` is running, and multiple threads may not call `config_log`
///   simultaneously.
/// * The provided `callback` itself function has two requirements:
///     * It must not invoke any SQLite calls.
///     * It must be threadsafe if SQLite is used in a multithreaded way.
///
/// cf [The Error And Warning Log](http://sqlite.org/errlog.html).
pub unsafe fn config_log(callback: Option<fn(c_int, &str)>) -> Result<()> {
    extern "C" fn log_callback(p_arg: *mut c_void, err: c_int, msg: *const c_char) {
        let c_slice = unsafe { CStr::from_ptr(msg).to_bytes() };
        let callback: fn(c_int, &str) = unsafe { mem::transmute(p_arg) };

        let s = String::from_utf8_lossy(c_slice);
        let _ = catch_unwind(|| callback(err, &s));
    }

    let rc = match callback {
        Some(f) => ffi::sqlite3_config(
            ffi::SQLITE_CONFIG_LOG,
            log_callback as extern "C" fn(_, _, _),
            f as *mut c_void,
        ),
        None => {
            let nullptr: *mut c_void = ptr::null_mut();
            ffi::sqlite3_config(ffi::SQLITE_CONFIG_LOG, nullptr, nullptr)
        }
    };

    if rc == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(error_from_sqlite_code(rc, None))
    }
}

/// `feature = "trace"` Write a message into the error log established by
/// `config_log`.
#[inline]
pub fn log(err_code: c_int, msg: &str) {
    let msg = CString::new(msg).expect("SQLite log messages cannot contain embedded zeroes");
    unsafe {
        ffi::sqlite3_log(err_code, b"%s\0" as *const _ as *const c_char, msg.as_ptr());
    }
}

// struct FancyThingy {
//     b: Box<dyn Fn(&str)>,
// }

impl Connection {
    /// `feature = "trace"` Register or clear a callback function that can be
    /// used for tracing the execution of SQL statements.
    ///
    /// Prepared statement placeholders are replaced/logged with their assigned
    /// values. There can only be a single tracer defined for each database
    /// connection. Setting a new tracer clears the old one.
    pub fn trace(&mut self, trace_fn: Option<fn(&str)>) {
        unsafe extern "C" fn trace_callback(p_arg: *mut c_void, z_sql: *const c_char) {
            let trace_fn: fn(&str) = mem::transmute(p_arg);
            let c_slice = CStr::from_ptr(z_sql).to_bytes();
            let s = String::from_utf8_lossy(c_slice);
            let _ = catch_unwind(|| trace_fn(&s));
        }

        let c = self.db.borrow_mut();
        match trace_fn {
            Some(f) => unsafe {
                ffi::sqlite3_trace(c.db(), Some(trace_callback), f as *mut c_void);
            },
            None => unsafe {
                ffi::sqlite3_trace(c.db(), None, ptr::null_mut());
            },
        }
    }

    /// `feature = "trace"` Register or clear a callback function that can be
    /// used for tracing the execution of SQL statements.
    ///
    /// Prepared statement placeholders are replaced/logged with their assigned
    /// values. There can only be a single tracer defined for each database
    /// connection. Setting a new tracer clears the old one.
    ///
    /// The fanciness with Box is to be able to cast to *c_void, and the 'a and 'b lifetimes are to
    /// guarantee that the closure lives as long as the Connection (this lifetime constraint isn't
    /// necessary but it is sufficient).
    pub fn trace_using_closure<'a,'b>(&'a mut self, trace_fn: Option<&'b Box<dyn Fn(&str)>>) where 'b: 'a {
        unsafe extern "C" fn trace_callback(p_arg: *mut c_void, z_sql: *const c_char) {
            let recovered_trace_fn = p_arg as *const Box<dyn Fn(&str)>;
            let c_slice = CStr::from_ptr(z_sql).to_bytes();
            let s = String::from_utf8_lossy(c_slice);
//             println!("trace callback called with p_arg = {:#?}, z_sql = {:#?}, s = {:#?}", p_arg, z_sql, s);
            // TODO: Figure out how to avoid error "error[E0277]: the type `dyn for<'r> Fn(&'r str)`
            // may contain interior mutability and a reference may not be safely transferrable across
            // a catch_unwind boundary"
//             let _ = catch_unwind(move || recovered_trace_fn.as_ref().unwrap()(&s));
            recovered_trace_fn.as_ref().unwrap()(&s);
        }

        let c = self.db.borrow_mut();
        match trace_fn {
            Some(f) => unsafe {
//                 println!("trace installed a callback");
                ffi::sqlite3_trace(c.db(), Some(trace_callback), f as *const _ as *mut c_void);
            },
            None => unsafe {
//                 println!("trace uninstalled a callback");
                ffi::sqlite3_trace(c.db(), None, ptr::null_mut());
            },
        }
    }

    /// `feature = "trace"` Register or clear a callback function that can be
    /// used for tracing the execution of SQL statements.
    ///
    /// Prepared statement placeholders are replaced/logged with their assigned
    /// values. There can only be a single tracer defined for each database
    /// connection. Setting a new tracer clears the old one.
    ///
    /// The fanciness with Box is to be able to cast to *c_void, and the use of Arc is to
    /// be able to check that the closure is still alive each time it's used.  This might
    /// be slower than is necessary.
    pub fn trace_using_closure_2(&mut self, trace_fn: Option<Arc<Box<dyn Fn(&str)>>>) {
        unsafe extern "C" fn trace_callback(p_arg: *mut c_void, z_sql: *const c_char) {
            let recovered_trace_fn_w = Weak::from_raw(p_arg as *const Box<dyn Fn(&str)>);
            if let Some(recovered_trace_fn_a) = recovered_trace_fn_w.upgrade() {
                let c_slice = CStr::from_ptr(z_sql).to_bytes();
                let s = String::from_utf8_lossy(c_slice);
    //             println!("trace callback called with p_arg = {:#?}, z_sql = {:#?}, s = {:#?}", p_arg, z_sql, s);
                // TODO: Figure out how to avoid error "error[E0277]: the type `dyn for<'r> Fn(&'r str)`
                // may contain interior mutability and a reference may not be safely transferrable across
                // a catch_unwind boundary"
    //             let _ = catch_unwind(move || recovered_trace_fn_a(&s));
                recovered_trace_fn_a(&s);
            } else {
                panic!("trace_fn was deallocated");
            }
            // From what I read of the docs, this is necessary so that each call to Weak::from_raw
            // is paired with exactly one call to into_raw.
            recovered_trace_fn_w.into_raw();
        }

        let c = self.db.borrow_mut();
        match trace_fn {
            Some(f) => unsafe {
//                 println!("trace installed a callback");
                ffi::sqlite3_trace(c.db(), Some(trace_callback), Arc::downgrade(&f).as_ptr() as *mut c_void);
            },
            None => unsafe {
//                 println!("trace uninstalled a callback");
                ffi::sqlite3_trace(c.db(), None, ptr::null_mut());
            },
        }
    }

//     /// `feature = "trace"` Register or clear a callback function that can be
//     /// used for tracing the execution of SQL statements.
//     ///
//     /// Prepared statement placeholders are replaced/logged with their assigned
//     /// values. There can only be a single tracer defined for each database
//     /// connection. Setting a new tracer clears the old one.
//     ///
//     /// TODO: Add mask and data pointer
//     /// See https://www.sqlite.org/c3ref/trace_v2.html and https://www.sqlite.org/c3ref/c_trace.html
// //     pub fn trace_v2(&mut self, trace_fn: Option<Box<FancyThingy>>) {
//     // NOTE that `'b: 'a` means that 'b must outlive 'a.
//     pub fn trace_v2<'a,'b>(&'a mut self, trace_fn: Option<&'b Box<dyn Fn(&str)>>) where 'b: 'a {
//         // This is the actual callback passed into sqlite3_trace_v2.  It acts as the glue layer
//         unsafe extern "C" fn trace_callback(_mask: c_uint, _c: *mut c_void, p_arg: *mut c_void, z_sql: *mut c_void) -> c_int {
// //             println!("HIPPO HIPPO");
// //             let recovered_trace_fn = Box::<dyn Fn(&str)>::from_raw(mem::transmute::<_,*mut dyn Fn(&str)>(p_arg));
//             let recovered_trace_fn = p_arg as *const Box<dyn Fn(&str)>;
//             let c_slice = CStr::from_ptr(z_sql as *const c_char).to_bytes();
//             let s = String::from_utf8_lossy(c_slice);
// //             println!("HIPPO; s: {:#?}", s);
// //             let _ = catch_unwind(|| (*recovered_trace_fn)(&s));
//             // TEMP HACK -- disable catch_unwind for now
// //             (*recovered_trace_fn)(&s);
//             recovered_trace_fn.as_ref().unwrap()(&s);
//             0 // Arbitrary return value -- TODO: Figure out if it should be specified
//         }
//
//         let c = self.db.borrow_mut();
//         match trace_fn {
//             Some(f) => unsafe {
// //                 println!("OSTRICH1");
//                 ffi::sqlite3_trace_v2(c.db(), ffi::SQLITE_TRACE_STMT as u32, Some(trace_callback), f as *const _ as *mut c_void);
//             },
//             None => unsafe {
// //                 println!("OSTRICH2");
//                 ffi::sqlite3_trace_v2(c.db(), 0, None, ptr::null_mut());
//             },
//         }
//     }

    /// `feature = "trace"` Register or clear a callback function that can be
    /// used for profiling the execution of SQL statements.
    ///
    /// There can only be a single profiler defined for each database
    /// connection. Setting a new profiler clears the old one.
    pub fn profile(&mut self, profile_fn: Option<fn(&str, Duration)>) {
        unsafe extern "C" fn profile_callback(
            p_arg: *mut c_void,
            z_sql: *const c_char,
            nanoseconds: u64,
        ) {
            let profile_fn: fn(&str, Duration) = mem::transmute(p_arg);
            let c_slice = CStr::from_ptr(z_sql).to_bytes();
            let s = String::from_utf8_lossy(c_slice);
            const NANOS_PER_SEC: u64 = 1_000_000_000;

            let duration = Duration::new(
                nanoseconds / NANOS_PER_SEC,
                (nanoseconds % NANOS_PER_SEC) as u32,
            );
            let _ = catch_unwind(|| profile_fn(&s, duration));
        }

        let c = self.db.borrow_mut();
        match profile_fn {
            Some(f) => unsafe {
                ffi::sqlite3_profile(c.db(), Some(profile_callback), f as *mut c_void)
            },
            None => unsafe { ffi::sqlite3_profile(c.db(), None, ptr::null_mut()) },
        };
    }
}

#[cfg(test)]
mod test {
    use lazy_static::lazy_static;
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::{Connection, Result};

    #[test]
    fn test_trace() -> Result<()> {
        lazy_static! {
            static ref TRACED_STMTS: Mutex<Vec<String>> = Mutex::new(Vec::new());
        }
        fn tracer(s: &str) {
            let mut traced_stmts = TRACED_STMTS.lock().unwrap();
            traced_stmts.push(s.to_owned());
        }

        let mut db = Connection::open_in_memory()?;
        db.trace(Some(tracer));
        {
            let _ = db.query_row("SELECT ?", [1i32], |_| Ok(()));
            let _ = db.query_row("SELECT ?", ["hello"], |_| Ok(()));
        }
        db.trace(None);
        {
            let _ = db.query_row("SELECT ?", [2i32], |_| Ok(()));
            let _ = db.query_row("SELECT ?", ["goodbye"], |_| Ok(()));
        }

        let traced_stmts = TRACED_STMTS.lock().unwrap();
        assert_eq!(traced_stmts.len(), 2);
        assert_eq!(traced_stmts[0], "SELECT 1");
        assert_eq!(traced_stmts[1], "SELECT 'hello'");
        Ok(())
    }

    #[test]
    fn test_profile() -> Result<()> {
        lazy_static! {
            static ref PROFILED: Mutex<Vec<(String, Duration)>> = Mutex::new(Vec::new());
        }
        fn profiler(s: &str, d: Duration) {
            let mut profiled = PROFILED.lock().unwrap();
            profiled.push((s.to_owned(), d));
        }

        let mut db = Connection::open_in_memory()?;
        db.profile(Some(profiler));
        db.execute_batch("PRAGMA application_id = 1")?;
        db.profile(None);
        db.execute_batch("PRAGMA application_id = 2")?;

        let profiled = PROFILED.lock().unwrap();
        assert_eq!(profiled.len(), 1);
        assert_eq!(profiled[0].0, "PRAGMA application_id = 1");
        Ok(())
    }
}
