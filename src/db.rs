use crate::errors::GDErr;
use std::{
    collections::HashMap,
    io::{Read, Write},
};

async fn download(url: String, file: String) -> Result<(), GDErr> {
    let res = reqwest::get(url).await?.bytes().await?;
    let mut f = std::fs::File::create(file)?;
    f.write_all(res.as_ref())?;
    Ok(())
}

fn update_needed(dir: String) -> bool {
    let p = std::path::Path::new(&dir).join("version");
    if cfg!(feature = "noupdate") {
        if p.exists() {
            println!("feature noupdate is active");
            return false;
        }
    }
    let mut f = match std::fs::File::open(p) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{:?}", e);
            return true;
        }
    };
    let mut s = String::new();
    match f.read_to_string(&mut s) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{:?}", e);
            return true;
        }
    }
    let secs = match s.parse::<u64>() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("{:?}", e);
            return true;
        }
    };
    let last = std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs);
    let ds = match std::time::SystemTime::now().duration_since(last) {
        Ok(ds) => ds,
        Err(e) => {
            eprintln!("{:?}", e);
            return true;
        }
    };
    if ds < std::time::Duration::from_secs(3600 * 24 * 7) {
        eprintln!("It has been less than 7 days since last download");
        return false;
    }
    true
}

async fn get_files(dir: String) -> Result<(), GDErr> {
    std::fs::create_dir_all(&dir)?;
    let key = std::env::var("MAXMIND_KEY")?;

    // asn database
    if cfg!(feature = "asn") {
        let url = format!(
            "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN&license_key={}&suffix=tar.gz",
            key,
        );
        let f = if let Some(p) = std::path::Path::new(&dir).join("asn.tar.gz").to_str() {
            String::from(p)
        } else {
            return Err(GDErr::GenericErr(String::from("invalid path")));
        };
        println!("Downloading ASN database");
        download(url, f.clone()).await?;
        println!("Decompressing..");
        let tar = flate2::read::GzDecoder::new(std::fs::File::open(&f)?);
        let d = if let Some(p) = std::path::Path::new(&dir).join("asn").to_str() {
            String::from(p)
        } else {
            return Err(GDErr::GenericErr(String::from("invalid path")));
        };
        std::fs::create_dir_all(&d)?;
        println!("Unarchiving..");
        tar::Archive::new(tar).unpack(&d)?;
        println!("Done!!");
    }

    // cities
    if cfg!(feature = "cities") {
        let url = format!(
            "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key={}&suffix=tar.gz",
            key,
        );
        let f = if let Some(p) = std::path::Path::new(&dir).join("cities.tar.gz").to_str() {
            String::from(p)
        } else {
            return Err(GDErr::GenericErr(String::from("invalid path")));
        };
        println!("Downloading Cities database");
        download(url, f.clone()).await?;
        println!("Decompressing..");
        let tar = flate2::read::GzDecoder::new(std::fs::File::open(&f)?);
        let d = if let Some(p) = std::path::Path::new(&dir).join("cities").to_str() {
            String::from(p)
        } else {
            return Err(GDErr::GenericErr(String::from("invalid path")));
        };
        std::fs::create_dir_all(&d)?;
        println!("Unarchiving..");
        tar::Archive::new(tar).unpack(&d)?;
        println!("Done!!");
    }

    // countries
    let url = format!(
        "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-Country&license_key={}&suffix=tar.gz",
        key,
    );
    let f = if let Some(p) = std::path::Path::new(&dir).join("countries.tar.gz").to_str() {
        String::from(p)
    } else {
        return Err(GDErr::GenericErr(String::from("invalid path")));
    };
    println!("Downloading Countries database");
    download(url, f.clone()).await?;
    println!("Decompressing..");
    let tar = flate2::read::GzDecoder::new(std::fs::File::open(&f)?);
    let d = if let Some(p) = std::path::Path::new(&dir).join("countries").to_str() {
        String::from(p)
    } else {
        return Err(GDErr::GenericErr(String::from("invalid path")));
    };
    std::fs::create_dir_all(&d)?;
    println!("Unarchiving..");
    tar::Archive::new(tar).unpack(&d)?;
    println!("Done!!");

    // Countries ISO CSV
    let url =
        "https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv";
    let f = if let Some(p) = std::path::Path::new(&dir)
        .join("countries-iso.csv")
        .to_str()
    {
        String::from(p)
    } else {
        return Err(GDErr::GenericErr(String::from(
            "invalid path for countries-iso.csv",
        )));
    };
    println!("Downloading countries ISO");
    download(url.to_string(), f.clone()).await?;
    println!("Done!!");
    Ok(())
}

fn record_to_string(record: &csv::StringRecord, index: usize) -> Result<String, GDErr> {
    let d = if let Some(v) = record.get(index) {
        String::from(v)
    } else {
        return Err(GDErr::GenericErr("failed to convert to string".to_string()));
    };
    Ok(d)
}

fn record_to_i32(record: &csv::StringRecord, index: usize) -> Option<i32> {
    if let Some(v) = record.get(index) {
        match v.parse::<i32>() {
            Ok(v) => {
                return Some(v);
            }
            Err(e) => {
                eprintln!("{:?}", e);
            }
        };
    };
    None
}

fn record_to_i64(record: &csv::StringRecord, index: usize) -> Option<i64> {
    if let Some(v) = record.get(index) {
        match v.parse::<i64>() {
            Ok(v) => {
                return Some(v);
            }
            Err(e) => {
                eprintln!("{:?}", e);
            }
        };
    };
    None
}
fn record_to_vec_string(record: &csv::StringRecord, index: usize) -> Result<Vec<String>, GDErr> {
    let mut vs = Vec::<String>::new();
    if let Some(v) = record.get(index) {
        for item in String::from(v).split(',') {
            vs.push(String::from(item))
        }
    } else {
        return Err(GDErr::GenericErr("failed to convert to string".to_string()));
    };
    Ok(vs)
}

#[derive(Debug, Clone, Default)]
pub struct Country {
    pub dial_codes: Vec<String>,     // 1
    pub iso3: String,                // 2
    pub iso_num: Option<i32>,        // 5
    pub iso2: String,                // 9
    pub currency_name: String,       // 18
    pub currency_code: String,       // 25
    pub name: String,                // 41
    pub region: String,              // 44
    pub capital: String,             // 49
    pub continent_code: String,      // 50
    pub tld: String,                 // 51
    pub language_codes: Vec<String>, // 52
    pub geoname_id: Option<i64>,     // 53
    pub display_name: String,        // 54
}
fn csv_to_countries(f: String) -> Result<HashMap<String, Country>, GDErr> {
    let mut countries = HashMap::<String, Country>::new();
    let mut r = csv::Reader::from_path(&f)?;
    for result in r.records() {
        let record = result?;
        let country = Country {
            dial_codes: record_to_vec_string(&record, 1)?,
            iso3: record_to_string(&record, 2)?,
            iso_num: record_to_i32(&record, 5),
            iso2: record_to_string(&record, 9)?,
            currency_name: record_to_string(&record, 18)?,
            currency_code: record_to_string(&record, 25)?,
            name: record_to_string(&record, 41)?,
            region: record_to_string(&record, 44)?,
            capital: record_to_string(&record, 49)?,
            continent_code: record_to_string(&record, 50)?,
            tld: record_to_string(&record, 51)?,
            language_codes: record_to_vec_string(&record, 52)?,
            geoname_id: record_to_i64(&record, 53),
            display_name: record_to_string(&record, 54)?,
        };
        countries.insert(String::from(&country.iso2), country);
    }
    Ok(countries)
}

pub async fn update_db() -> Result<String, GDErr> {
    let dir = std::env::var("GL2_DBDIR")?;
    std::fs::create_dir_all(&dir)?;
    if !update_needed(dir.clone()) {
        return Ok("nochange".to_string());
    }
    let version_file = std::path::Path::new(&dir).join("version.new");
    let mut vf = std::fs::File::create(&version_file)?;
    vf.write_all(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            .to_string()
            .as_bytes(),
    )?;
    let scratch_dir = format!("{}/scratch", dir.trim_end_matches('/'));
    get_files(scratch_dir.clone()).await?;

    // overwrite existing dbs
    std::fs::create_dir_all(std::path::Path::new(&dir).join("dbs"))?;

    // move asn files
    if cfg!(feature = "asn") {
        let p = std::path::Path::new(&scratch_dir).join("asn");
        let mut p = if let Some(v) = std::fs::read_dir(p)?.next() {
            match v {
                Ok(v) => v.path(),
                Err(e) => {
                    return Err(GDErr::IOErr(e));
                }
            }
        } else {
            return Err(GDErr::GenericErr(String::from("invalid path /asn")));
        };
        p.push("GeoLite2-ASN.mmdb");
        std::fs::copy(p, std::path::Path::new(&dir).join("dbs/asn.mmdb"))?;
    }

    // move cities files
    if cfg!(feature = "cities") {
        let p = std::path::Path::new(&scratch_dir).join("cities");
        let mut p = if let Some(v) = std::fs::read_dir(p)?.next() {
            match v {
                Ok(v) => v.path(),
                Err(e) => {
                    return Err(GDErr::IOErr(e));
                }
            }
        } else {
            return Err(GDErr::GenericErr(String::from("invalid path /cities")));
        };
        p.push("GeoLite2-City.mmdb");
        std::fs::copy(p, std::path::Path::new(&dir).join("dbs/cities.mmdb"))?;
    }

    // move country files
    let p = std::path::Path::new(&scratch_dir).join("countries");
    let mut p = if let Some(v) = std::fs::read_dir(p)?.next() {
        match v {
            Ok(v) => v.path(),
            Err(e) => {
                return Err(GDErr::IOErr(e));
            }
        }
    } else {
        return Err(GDErr::GenericErr(String::from("invalid path /countries")));
    };
    p.push("GeoLite2-Country.mmdb");
    std::fs::copy(p, std::path::Path::new(&dir).join("dbs/countries.mmdb"))?;

    let p = std::path::Path::new(&scratch_dir).join("countries-iso.csv");
    std::fs::copy(p, std::path::Path::new(&dir).join("dbs/countries-iso.csv"))?;

    // Rename version - so that download does not occur again
    std::fs::rename(&version_file, std::path::Path::new(&dir).join("version"))?;

    // Delete scratch files
    std::fs::remove_dir_all(&scratch_dir)?;
    Ok("updated".to_string())
}

#[cfg(feature = "cities")]
pub async fn reader_cities() -> Result<maxminddb::Reader<Vec<u8>>, GDErr> {
    let dir = std::env::var("GL2_DBDIR")?;
    let p = std::path::Path::new(&dir).join("dbs/cities.mmdb");
    let reader = maxminddb::Reader::open_readfile(&p)?;
    Ok(reader)
}

pub async fn reader_countries() -> Result<maxminddb::Reader<Vec<u8>>, GDErr> {
    let dir = std::env::var("GL2_DBDIR")?;
    let p = std::path::Path::new(&dir).join("dbs/countries.mmdb");
    let reader = maxminddb::Reader::open_readfile(&p)?;
    Ok(reader)
}

#[cfg(feature = "asn")]
pub async fn reader_asn() -> Result<maxminddb::Reader<Vec<u8>>, GDErr> {
    let dir = std::env::var("GL2_DBDIR")?;
    let p = std::path::Path::new(&dir).join("dbs/asn.mmdb");
    let reader = maxminddb::Reader::open_readfile(&p)?;
    Ok(reader)
}

pub async fn countries_hashmap() -> Result<HashMap<String, Country>, GDErr> {
    let dir = std::env::var("GL2_DBDIR")?;
    let p = if let Some(p) = std::path::Path::new(&dir)
        .join("dbs/countries-iso.csv")
        .to_str()
    {
        String::from(p)
    } else {
        return Err(GDErr::GenericErr("path to string failed".to_string()));
    };
    csv_to_countries(p)
}

pub async fn sync_dbs(tx: Option<tokio::sync::mpsc::Sender<String>>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
    loop {
        let _i = interval.tick().await;
        match update_db().await {
            Ok(v) => {
                if let Some(t) = &tx {
                    let _ = t.send(v).await;
                }
            }
            Err(e) => {
                eprintln!("{:?}", e);
                if let Some(t) = &tx {
                    let _ = t.send("error".to_string()).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{countries_hashmap, update_db, GDErr};

    #[tokio::test]
    async fn get_countries_hashmap() {
        update_db().await.unwrap();
        let c = countries_hashmap().await.unwrap();
        let c = c.get("IN").unwrap();
        let c = c.dial_codes.get(0).unwrap();
        assert_eq!(c, "91");
    }
}
