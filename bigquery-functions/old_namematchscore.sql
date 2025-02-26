CREATE OR REPLACE FUNCTION `sync_metadata.namematchscore`(aname string, bname string, aage int64, bage int64, agender string, bgender string, adob string, bdob string, cyear int64)
RETURNS INT64
LANGUAGE js AS """

function levenshteinDistance(str1, str2) {
    const len1 = str1.length;
    const len2 = str2.length;

    const matrix = [];

    for (let i = 0; i <= len1; i++) {
        matrix[i] = [i];
    }
    for (let j = 0; j <= len2; j++) {
        matrix[0][j] = j;
    }

    for (let i = 1; i <= len1; i++) {
        for (let j = 1; j <= len2; j++) {
            const cost = (str1.charAt(i - 1) === str2.charAt(j - 1)) ? 0 : 1;
            matrix[i][j] = Math.min(
                matrix[i-1][j] + 1,
                matrix[i][j-1] + 1,
                matrix[i-1][j-1] + cost
            );
        }
    }
    return matrix[len1][len2];
}

function checkrelations(name) {
    let rel_list = ['baby', 'bo', 'capt', 'wo', 'so', 'do', 'co', 'mo', 'ho', 'fo', 'baby of'];
    let status = true;
    
    for (let rel of rel_list) {
        let initrel = rel + ' ';
        let midrel = ' ' + rel + ' ';
        
        if (name.indexOf(initrel) === 0 || name.indexOf(midrel) >= 0) {
            status = false;
        }
    }
    return status;
}

function namecleanup(name) {
    let temp_aname = name;
    let nl = name.length;
    let sal_list = ['ms', 'mrs', 'mr', 'miss'];
    let rel_list = [' baby ', ' bo ', ' capt ', ' wo ', ' so ', ' do ', ' co ', ' mo ', ' ho ', ' fo ', ' baby of '];
    
    for (let rel of rel_list) {
        let relpos = temp_aname.indexOf(rel);
        if (relpos > 0) {
            let beforrel = temp_aname.slice(0, relpos).trim();
            if (beforrel.length > 0) {
                temp_aname = beforrel;
            }
        }
    }
    
    for (let sal of sal_list) {
        let initsal = sal + ' ';
        let midsal = ' ' + sal + ' ';
        if (temp_aname.startsWith(initsal)) {
            let sidx = initsal.length;
            return temp_aname.slice(sidx);
        }
        
        let pos = temp_aname.indexOf(midsal);
        if (pos >= 0) {
            let sidx = pos + midsal.length;
            let beforesal = temp_aname.slice(0, pos).trim();
            let aftersal = temp_aname.slice(sidx).trim();
            let aftersal_words = aftersal.split(' ').filter(n => n);
            if (aftersal_words.length >= 2 && checkrelations(beforesal)) {
                return aftersal;
            }
        }
    }
    
    return temp_aname;
}

function check_special_cases(rcase, name1, name2) {
    let case1 = " " + rcase + " ";
    let case2 = rcase + " ";
    if ((name2.includes(case1) || name2.startsWith(case2)) && 
        (!name1.includes(case1) && !name1.startsWith(case2))) {
        return true;
    }
    return false;
}

function nms(aname, bname, aage, bage, agender, bgender, adob, bdob, cyear) {
	if (aname === null || bname === null) {
        return 0;
    }
	
	let special_cases = ['baby', 'bo', 'capt', 'wo', 'so', 'do', 'co', 'mo', 'ho', 'fo', 'baby of']
	
	aname = namecleanup(aname);
    bname = namecleanup(bname);
	let allowed_age = 3;
    let inner_loop_idx = 1;
    let full_match_status = 1;
    let match_list = [];
    let temp_aname = aname;
    let temp_bname = bname;
    let word_matchcount = 0;
    let afullmatch = 0;
    let ainitialmatch = 0;
    let asimilarmatch = 0;
    let anearsimilarmatch = 0;
    let anearmatch = 0;
    let apartialmatch = 0;
    let score = 0;
    let currentmatch = 0;
    const NOMATCH = 0;
    const EXACT = 1;
    const DMETA = 2;
    const SOUND = 3;
    const SIMILAR = 4;
    const INITIALS = 5;
    const SUBSTR = 6;
    const DIST1 = 7;
    const SOUND1 = 8;
    const DIST2 = 9;

    if (aname.length > bname.length) {
        temp_bname = aname;
        temp_aname = bname;
    }
    if ((aname.split(" ").filter(x => x !== "").length > 2 || 
         bname.split(" ").filter(x => x !== "").length > 2) && 
        (agender === null || bgender === null || agender !== bgender)) {
        return 0;
    }
	
	allowed_age = (aage !== null && bage !== null && 
                   (cyear - aage) >= 50 && (cyear - bage) >= 50) ? 15 : 3;

    for (let caseItem of special_cases) {
        if (check_special_cases(caseItem, temp_aname, temp_bname)) {
            let scase = caseItem + " ";
            if (agender === null || bgender === null || agender !== bgender) {
                return 0;
            } else if (temp_bname.startsWith(scase)) {
                return 0;
            } else {
                allowed_age = 1;
            }
        }
        if (check_special_cases(caseItem, temp_bname, temp_aname)) {
            let scase = caseItem + " ";
            if (agender === null || bgender === null || agender !== bgender) {
                return 0;
            } else if (temp_aname.startsWith(scase)) {
                return 0;
            } else {
                allowed_age = 1;
            }
        }
    }
	
	let temp_first_aname = temp_aname.split(" ")[0];
    let temp_first_bname = temp_bname.split(" ")[0];
    if (temp_first_aname === temp_first_bname && 
        adob !== null && bdob !== null && adob === bdob) {
        return SIMILAR;
    }

    let taname = temp_aname.replace(" ", "");
    let tbname = temp_bname.replace(" ", "");
    let ballowed = allowed_age < 2 ? allowed_age : 2;
    if (taname.length >= 1 && tbname.startsWith(taname) && 
        ((aage === null || bage === null || aage === bage) || 
         ((aage === null || bage === null || Math.abs(aage - bage) <= ballowed) && 
          (agender === null || bgender === null || agender === bgender)))) {
        return 1;
    }

    let a_name_list = aname.split(' ').filter(n => n);
    let b_name_list = bname.split(' ').filter(n => n);
    let alen = a_name_list.length
    let blen = b_name_list.length;
    temp_aname = aname;
    temp_bname = bname;
    if (alen > blen) {
        [temp_aname, temp_bname] = [temp_bname, temp_aname];
        [a_name_list, b_name_list] = [b_name_list, a_name_list];
        [alen, blen] = [blen, alen];
    }

    if (b_name_list[0] === a_name_list[0] && alen === 2 && blen === 2) {
        if (sync_metadata.dmetaphonematch(b_name_list[1], a_name_list[1]) || levenshteinDistance(a_name_list[1], b_name_list[1]) === 1) {
            return 4;
        }
    }
	for (let i = 0; i < alen; i++) {
        if (i === 0) {
            if (a_name_list[i] === b_name_list[i] && 
                (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
                continue;
            } else {
                full_match_status = 0;
                break;
            }
        }
        if (a_name_list[i] === b_name_list[i] && 
            (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
            continue;
        }
        if (levenshteinDistance(a_name_list[i], b_name_list[i]) <= 1 && 
            a_name_list[i].length > 2 && b_name_list[i].length > 2 && 
            (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
            continue;
        }
        if (sync_metadata.dmetaphonematch(a_name_list[i], b_name_list[i]) && levenshteinDistance(a_name_list[i], b_name_list[i]) <= 2 && 
            (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
            continue;
        }
		if (levenshteinDistance(a_name_list[i], b_name_list[i]) <= 2 && a_name_list[i][0] === b_name_list[i][0] && (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
			continue;
		}
        if ((b_name_list[i].startsWith(a_name_list[i]) || 
             a_name_list[i].startsWith(b_name_list[i])) && 
            a_name_list[i].length > 1 && b_name_list[i].length > 1 && 
            (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
            continue;
        }
        if ((a_name_list[i].length === 1 || b_name_list[i].length === 1) && 
            (aage === null || bage === null || Math.abs(aage - bage) <= 2) && 
            (a_name_list[i][0] === b_name_list[i][0])) {
            continue;
        }
        if ((a_name_list[i].length === 2 || b_name_list[i].length === 2) && 
            (a_name_list[i][0] === b_name_list[i][0]) && 
            a_name_list[i].slice(-1) === b_name_list[i].slice(-1) && 
            (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
            continue;
        }
        full_match_status = 0;
        break;
    }

    if (full_match_status === 1) {
        return 1;
    }

    b_name_list.sort((a, b) => b.length - a.length);

    for (let a_part_new of a_name_list) {
        inner_loop_idx = 1;
        for (let b_part_new of b_name_list) {
            if (match_list.includes(inner_loop_idx)) {
                inner_loop_idx++;
                continue;
            }
            currentmatch = NOMATCH;
            let a_part_len = a_part_new.length;
            let b_part_len = b_part_new.length;

            let a_part, b_part;
            let a_part_pos, b_part_pos;

            if (a_part_len > b_part_len) {
                a_part = b_part_new;
                b_part = a_part_new;
                a_part_len = a_part.length;
                b_part_len = b_part.length;

                a_part_pos = temp_bname.split(" ").indexOf(b_part_new);
                b_part_pos = temp_aname.split(" ").indexOf(a_part_new);
            } else {
                a_part = a_part_new;
                b_part = b_part_new;
                a_part_pos = temp_aname.split(" ").indexOf(a_part_new);
                b_part_pos = temp_bname.split(" ").indexOf(b_part_new);
            }

            // Exact match case
            if ((a_part === b_part || 
                 a_part.replace("w", "v") === b_part.replace("w", "v")) && 
                (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
                currentmatch = EXACT;
                match_list.push(inner_loop_idx);
                if (a_part_len > 2 && b_part_len > 2) {
                    word_matchcount++;
                }
                break;
            }

            // Initials match case
            if ((a_part_len === 1 || b_part_len === 1) && 
                (aage === null || bage === null || Math.abs(aage - bage) <= 2)) {
                if (a_part[0] === b_part[0] && a_part_pos === b_part_pos) {
                    currentmatch = INITIALS;
                    match_list.push(inner_loop_idx);
                    break;
                }
            }

            // 14-jan 2 character words in name other than first name
            if ((a_part_len === 2 || b_part_len === 2) && 
                a_part_pos === b_part_pos && a_part_pos !== 1 && 
                (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
                if (a_part[0] === b_part[0] && a_part.slice(-1) === b_part.slice(-1)) {
                    currentmatch = INITIALS;
                }
                match_list.push(inner_loop_idx);
                break;
            }
			
			if(sync_metadata.dmetaphonematch(a_part, b_part) && levenshteinDistance(a_part, b_part) <= 2 && (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
				currentmatch = SOUND;
				match_list.push(inner_loop_idx);
				break;
			}

            // Substring match
            if (b_part.startsWith(a_part) && a_part.length > 1 && 
                (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
                currentmatch = SUBSTR;
                match_list.push(inner_loop_idx);
                break;
            }

            let abdist = levenshteinDistance(a_part, b_part);

            if (abdist === 1 && a_part_len >= 4 && b_part_len >= 4 && 
                (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
                currentmatch = DIST1;
                match_list.push(inner_loop_idx);
                break;
            }

            if (abdist === 2 && a_part_len >= 6 && 
                a_part.slice(0, 3) === b_part.slice(0, 3) && 
                (aage === null || bage === null || Math.abs(aage - bage) <= allowed_age)) {
                currentmatch = DIST2;
                match_list.push(inner_loop_idx);
                break;
            }

            inner_loop_idx++;
        }
        if (currentmatch === EXACT) {
            afullmatch++;
        }
        if (currentmatch === INITIALS) {
            ainitialmatch++;
        }
        if (currentmatch === SOUND || currentmatch === SUBSTR) {
            asimilarmatch++;
        }
        if (currentmatch === DMETA) {
            anearsimilarmatch++;
        }
        if (currentmatch === DIST1) {
            anearmatch++;
        }
        if (currentmatch === SOUND1 || currentmatch === DIST2) {
            apartialmatch++;
        }
    }
    if (afullmatch === 0 && asimilarmatch === 0 && alen > 1) {
        return NOMATCH;
    }
    let maxpos = Math.floor(alen / 2) > 1;
    if (afullmatch < maxpos || 
        (agender === null || bgender === null || agender !== bgender)) {
        return NOMATCH;
    }
    if (alen === 1 && 
        ((agender === null || bgender === null || agender !== bgender) || 
         (aage === null || bage === null || aage !== bage))) {
        return NOMATCH;
    }
    currentmatch = afullmatch;

    if (currentmatch === alen) {
        return 1;
    }
    currentmatch += asimilarmatch;
    if (currentmatch === alen && blen === alen) {
        return 2;
    }
    if (currentmatch === alen && 
        (aage !== null && bage !== null && Math.abs(aage - bage) <= allowed_age)) {
        return 2;
    }
    currentmatch += ainitialmatch;
    if (currentmatch === alen && blen === alen) {
        return 3;
    }
    if (currentmatch === alen && 
        (aage !== null && bage !== null && Math.abs(aage - bage) <= allowed_age)) {
        return 3;
    }
    currentmatch += anearsimilarmatch;
    if (currentmatch === alen && blen === alen) {
        return 4;
    }
    if (currentmatch === alen && 
        (aage !== null && bage !== null && Math.abs(aage - bage) <= allowed_age)) {
        return 4;
    }
    currentmatch += anearmatch;
    if (currentmatch === alen && blen === alen) {
        return 5;
    }
    if (currentmatch === alen && 
        (aage !== null && bage !== null && Math.abs(aage - bage) <= allowed_age)) {
        return 5;
    }
    currentmatch += apartialmatch;
    if (currentmatch === alen && blen === alen) {
        return 6;
    }
    if (currentmatch === alen && 
        (aage !== null && bage !== null && Math.abs(aage - bage) <= allowed_age)) {
        return 6;
    }
    if (alen > 2 && blen > 2 && word_matchcount === 1) {
        return 17;
    }
    if (alen === 1 || alen < blen) {
        return NOMATCH;
    }
    alen--;
    currentmatch = afullmatch;

    if (currentmatch === alen) {
        return 11;
    }
    currentmatch += asimilarmatch;
    if (currentmatch === alen) {
        return 12;
    }
    currentmatch += ainitialmatch;
    if (currentmatch === alen) {
        return 13;
    }
    currentmatch += anearsimilarmatch;
    if (currentmatch === alen) {
        return 14;
    }
    currentmatch += anearmatch;
    if (currentmatch === alen) {
        return 15;
    }
    currentmatch += apartialmatch;
    if (currentmatch === alen) {
        return 16;
    }
    return NOMATCH;
}
return nms(aname, bname, aage, bage, agender, bgender, adob, bdob, cyear);
""";