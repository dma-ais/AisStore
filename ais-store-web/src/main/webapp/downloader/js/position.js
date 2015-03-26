

function nm2km(nm) {
    if (!nm) {
        return undefined;
    }
    return Math.round(nm * 1852 / 1000);
}

function km2nm(km) {
    if (!km) {
        return undefined;
    }
    return Math.round(km * 1000 / 1852);
}

function m2nm(m) {
    if (!m) {
        return undefined;
    }
    return Math.round(m / 1852);
}

function formatLongitude(longitude) {
    var ns = "E";
    if (longitude < 0) {
        ns = "W";
        longitude *= -1;
    }
    var hours = Math.floor(longitude);
    longitude -= hours;
    longitude *= 60;
    var lonStr = longitude.toFixed(3);
    while (lonStr.indexOf('.') < 2) {
        lonStr = "0" + lonStr;
    }

    return (hours / 1000.0).toFixed(3).substring(2) + " " + lonStr + ns;
}

function formatLatitude(latitude) {
    var ns = "N";
    if (latitude < 0) {
        ns = "S";
        latitude *= -1;
    }
    var hours = Math.floor(latitude);
    latitude -= hours;
    latitude *= 60;
    var latStr = latitude.toFixed(3);
    while (latStr.indexOf('.') < 2) {
        latStr = "0" + latStr;
    }

    return (hours / 100.0).toFixed(2).substring(2) + " " + latStr + ns;
}

function formatLonLat(lonlat) {
    return formatLatitude(lonlat.lat) + "  " + formatLongitude(lonlat.lon);
}

function parseLatitude(value) {
    if (value.trim().indexOf(" ") < 0) {
        var parsed = parseFloat(value);
        if (parsed == value) {
            return parsed;
        }
    }
    var parts = splitFormattedPos(value);
    return parseLat(parts[0], parts[1], parts[2]);
}

function parseLongitude(value) {
    if (value.trim().indexOf(" ") < 0) {
        var parsed = parseFloat(value);
        if (parsed == value) {
            return parsed;
        }
    }
    var parts = splitFormattedPos(value);
    return parseLon(parts[0], parts[1], parts[2]);
}

function splitFormattedPos(posStr) {
    var parts = [];
    parts[2] = posStr.substring(posStr.length - 1);
    posStr = posStr.substring(0, posStr.length - 1);
    var posParts = posStr.trim().split(" ");
    if (posParts.length != 2) {
        throw "Format exception";
    }
    parts[0] = posParts[0];
    parts[1] = posParts[1];
    return parts;
}

function parseString(str){
    str = str.trim();
    if (str == null || str.length == 0) {
        return null;
    }
    return str;
}

function parseLat(hours, minutes, northSouth) {
    var h = parseInt(hours, 10);
    var m = parseFloat(minutes);
    var ns = parseString(northSouth);
    if (h == null || m == null || ns == null) {
        throw "Format exception";
    }
    ns = ns.toUpperCase();
    if (!(ns == "N") && !(ns == "S")) {
        throw "Format exception";
    }
    var lat = h + m / 60.0;
    if (ns == "S") {
        lat *= -1;
    }
    return lat;
}

function parseLon(hours, minutes, eastWest) {
    var h = parseInt(hours, 10);
    var m = parseFloat(minutes);
    var ew = parseString(eastWest);
    if (h == null || m == null || ew == null) {
        throw "Format exception";
    }
    ew = ew.toUpperCase();
    if (!(ew == "E") && !(ew == "W")) {
        throw "Format exception";
    }
    var lon = h + m / 60.0;
    if (ew == "W") {
        lon *= -1;
    }
    return lon;
}

