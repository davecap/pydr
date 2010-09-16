# PEPA
set H102NE2 [ atomid PEPA 102 NE2 ]
set H102FE [ atomid PEPA 102 FE ]
set H419NE2 [ atomid PEPA 419 NE2 ]
set H419FE [ atomid PEPA 419 FE ]
set HSD411NE2 [ atomid PEPA 411 NE2 ]
set ASP412OD2 [ atomid PEPA 412 OD2 ]
set HSD333NE2 [ atomid PEPA 333 NE2 ]
set HSE284ND1 [ atomid PEPA 284 ND1 ]
set HSD334NE2 [ atomid PEPA 334 NE2 ]
set MG [ atomid I 1 MG ]
set CUB [ atomid I 3 CU ]

addatom $H102NE2
addatom $H102FE
addatom $H419NE2
addatom $H419FE
addatom $HSD411NE2
addatom $ASP412OD2
addatom $HSD333NE2
addatom $HSE284ND1
addatom $HSD334NE2
addatom $MG
addatom $CUB

# PEPB
set CUA1 [ atomid J 1 CU ]
set CUA2 [ atomid J 2 CU ]
set C252SG [ atomid PEPB 252 SG ]
set C256SG [ atomid PEPB 256 SG ]
set H217ND1 [ atomid PEPB 217 ND1 ]
set H260ND1 [ atomid PEPB 260 ND1 ]
set M263SD [ atomid PEPB 263 SD ]
set E254O [ atomid PEPB 254 O ]
set E254OE1 [ atomid PEPB 254 OE1 ]

addatom $CUA1
addatom $CUA2
addatom $C252SG
addatom $C256SG
addatom $H217ND1
addatom $H260ND1
addatom $M263SD
addatom $E254O
addatom $E254OE1

# 139 CHI1

set N139N [ atomid PEPA 139 N ]
# set N139HN [ atomid PEPA 139 HN ]
addatom $N139N

set N139CA [ atomid PEPA 139 CA ]
# set N139HA [ atomid PEPA 139 HA ]
addatom $N139CA

set N139CB [ atomid PEPA 139 CB ]
# set N139HB1 [ atomid PEPA 139 HB1 ]
# set N139HB1 [ atomid PEPA 139 HB2 ]
addatom $N139CB

set N139CG [ atomid PEPA 139 CG ]
set N139OD1 [ atomid PEPA 139 OD1 ]
set N139ND2 [ atomid PEPA 139 ND2 ]
# set N139HD21 [ atomid PEPA 139 HD21 ]
# set N139HD22 [ atomid PEPA 139 HD22 ]
addatom $N139CG
addatom $N139OD1
addatom $N139ND2
#set CGGROUP [addgroup { [ expr $N139CG ] $N139OD1 $N139ND2 } ]

print "CHI1K=$CHI1_K"
print "CHI1COORD=$CHI1_COORD"
    
# CHI1_K is set in kcal/mol/deg^2, convert it to kcal/mol/rad^2
set CHI1_K [expr $CHI1_K/0.0003045]
set CHI1_COORD [expr $CHI1_COORD*0.01745]

# Call procedure 
proc calcforces {} { 
    global H102NE2
    global H102FE
    global H419NE2
    global H419FE
    global HSD411NE2
    global ASP412OD2
    global HSD333NE2
    global HSE284ND1
    global HSD334NE2
    global MG
    global CUB
    
    global CUA1
    global CUA2
    global C252SG
    global C256SG
    global H217ND1
    global H260ND1
    global M263SD
    global E254O
    global E254OE1

    loadcoords c 

    # DIHEDRAL RESTRAINT START
    global N139N N139CA N139CB N139CG N139OD1 N139ND2
    
    global CHI1_K
    global CHI1_COORD
    
    # Apply dihedral restraint on CHI1 of residue 139
    
    # Calculate the current dihedral
    set cur_chi1 [getdihedral $c($N139N) $c($N139CA) $c($N139CB) $c($N139CG)]
    if {$cur_chi1 < 0} { set cur_chi1 [ expr $cur_chi1+360 ] }
    
    # Change to radian (PI/180 = 0.01745)
    set cur_chi1_rad [expr $cur_chi1*0.01745]
    print "CHI1DEG=$cur_chi1"
    print "CHI1RAD=$cur_chi1_rad"

    # (optional) Add this constraining energy to "MISC" in the energy output
    addenergy [expr $CHI1_K*$cur_chi1_rad*$cur_chi1_rad/2.0]

    # Calculate the "force" along the dihedral according to the harmonic constraint
    set force [expr -$CHI1_K*($cur_chi1_rad-$CHI1_COORD)]

    # Calculate the gradients
    foreach {g1 g2 g3 g4} [dihedralgrad $c($N139N) $c($N139CA) $c($N139CB) $c($N139CG)] {}

    # The force to be applied on each atom is proportional to its corresponding gradient
    addforce $N139N [vecscale $g1 $force]
    addforce $N139CA [vecscale $g2 $force]
    addforce $N139CB [vecscale $g3 $force]
    addforce $N139CG [vecscale $g4 $force]

    # DIHEDRAL RESTRAINT END
        
    # PEPA distance restraints
    
    #HDH102:NE2 <-> HDH102:FE 0.25
    set r0 2.0
    set k 50.0
    set r12 [ vecsub $c($H102NE2) $c($H102FE) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $H102NE2 [ vecscale [expr -1*$force] $n0 ] 
    addforce $H102FE [ vecscale $force $n0 ]
     
    # #HDH419:NE2 <-> HDH419:FE 0.25
    set r0 2.7
    set k 200.0
    set r12 [ vecsub $c($H419NE2) $c($H419FE) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $H419NE2 [ vecscale [expr -1*$force] $n0 ] 
    addforce $H419FE [ vecscale $force $n0 ]
    
    # CUB <-> HDH419:FE
    set r0 3.0
    set k 200.0
    set r12 [ vecsub $c($CUB) $c($H419FE) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $CUB [ vecscale [expr -1*$force] $n0 ] 
    addforce $H419FE [ vecscale $force $n0 ]
     
    #HSD411:NE2 <-> MG 0.25
    set r0 2.1
    set k 10.0
    set r12 [ vecsub $c($HSD411NE2) $c($MG) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $HSD411NE2 [ vecscale [expr -1*$force] $n0 ] 
    addforce $MG [ vecscale $force $n0 ]
    
    #ASP412:OD2 <-> MG 0.25
    set r0 1.8
    set k 10.0
    set r12 [ vecsub $c($ASP412OD2) $c($MG) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $ASP412OD2 [ vecscale $force $n0 ] 
    addforce $MG [ vecscale [expr -1*$force] $n0 ]
    
    #E254:OE1 <-> MG
    set r0 1.9
    set k 10.0
    set r12 [ vecsub $c($E254OE1) $c($MG) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $E254OE1 [ vecscale $force $n0 ] 
    addforce $MG [ vecscale [expr -1*$force] $n0 ]
    
    #HSD333:NE2 <-> CuB 5 0.22
    set r0 2.2
    set k 100.0
    set r12 [ vecsub $c($HSD334NE2) $c($CUB) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $HSD334NE2 [ vecscale [expr -1*$force] $n0 ] 
    addforce $CUB [ vecscale $force $n0 ]
    
    #HSE284:ND1 <-> CuB 5 0.22
    set r0 2.2
    set k 100.0
    set r12 [ vecsub $c($HSE284ND1) $c($CUB) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $HSE284ND1 [ vecscale [expr -1*$force] $n0 ] 
    addforce $CUB [ vecscale $force $n0 ]
    
    #HSD334:NE2 <-> CuB 5 0.22
    set r0 2.2
    set k 100.0
    set r12 [ vecsub $c($HSD334NE2) $c($CUB) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $HSD334NE2 [ vecscale [expr -1*$force] $n0 ]
    addforce $CUB [ vecscale $force $n0 ]
    
    # PEPB
    
    #CuA1 to CuA2 0.45
    set r0 4.5
    set k 100.0
    set r12 [ vecsub $c($CUA1) $c($CUA2) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $CUA1 [ vecscale [expr -1*$force] $n0 ] 
    addforce $CUA2 [ vecscale $force $n0 ]
    
    #CYS252:SG to CuA1 0.21
    set r0 2.1
    set k 500.0
    set r12 [ vecsub $c($C252SG) $c($CUA1) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $C252SG [ vecscale [expr -1*$force] $n0 ]
    addforce $CUA1 [ vecscale $force $n0 ]

    #CYS252:SG to CuA2 0.21
    set r0 2.1
    set k 500.0
    set r12 [ vecsub $c($C252SG) $c($CUA2) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $C252SG [ vecscale [expr -1*$force] $n0 ]
    addforce $CUA2 [ vecscale $force $n0 ]
    
    #CYS256:SG to CuA1 0.21
    set r0 2.1
    set k 500.0
    set r12 [ vecsub $c($C256SG) $c($CUA1) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $C256SG [ vecscale [expr -1*$force] $n0 ]
    addforce $CUA1 [ vecscale $force $n0 ]
    
    #CYS256:SG to CuA2 0.21
    set r0 2.1
    set k 500.0
    set r12 [ vecsub $c($C256SG) $c($CUA2) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $C256SG [ vecscale [expr -1*$force] $n0 ] 
    addforce $CUA2 [ vecscale $force $n0 ]
    
    #HSE217:ND1 to CuA2 0.24
    set r0 2.4
    set k 500.0
    set r12 [ vecsub $c($H217ND1) $c($CUA2) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $H217ND1 [ vecscale [expr -1*$force] $n0 ] 
    addforce $CUA2 [ vecscale $force $n0 ]
    
    #HSE260:ND1 to CuA1 0.243
    set r0 2.43
    set k 500.0
    set r12 [ vecsub $c($H260ND1) $c($CUA1) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $H260ND1 [ vecscale [expr -1*$force] $n0 ] 
    addforce $CUA1 [ vecscale $force $n0 ]
    
    #MET263:SD to CuA2 0.255
    set r0 2.55
    set k 500.0
    set r12 [ vecsub $c($M263SD) $c($CUA2) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $M263SD [ vecscale [expr -1*$force] $n0 ]
    addforce $CUA2 [ vecscale $force $n0 ]
    
    #GLU254:O to CuA1 0.235
    set r0 2.35
    set k 500.0
    set r12 [ vecsub $c($E254O) $c($CUA1) ]
    set r [ veclength $r12 ]
    set n0 [ vecnorm $r12 ]
    set force [ expr $k*($r-$r0) ]
    addforce $E254O [ vecscale [expr -1*$force] $n0 ]
    addforce $CUA1 [ vecscale $force $n0 ]
}

# There is a definition of subroutine veclength and vecnorm 
# Returns: the vector length 
proc veclength {v} { 
    set retval 0 
    foreach term $v { 
        set retval [expr $retval + $term * $term ] 
    } 
    return [expr sqrt($retval)] 
} 

# Returns: the normal vector 
proc vecnorm {v} { 
    set sum 0 
    foreach term $v { 
        set sum [expr $sum + $term*$term] 
    } 
    set sum [expr sqrt($sum)] 
    set retval {} 
    foreach term $v { 
        lappend retval [expr $term / $sum] 
    }
    return $retval 
} 
