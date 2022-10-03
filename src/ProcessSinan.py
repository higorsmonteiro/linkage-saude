# -*- coding: utf-8 -*- 
# Author: Higor S. Monteiro

'''
    Create the general class able to handle universal tasks on SINAN. 

    Objective: 
        Define I/O tasks to this general class. Thereafter, we should define 
        specific children classes for any disease of interest, such as: DENGUE,
        CHIKUNGUNYA, SARAMPO, etc. Children classes should be created and updated
        as long as demands are generated (or outbreaks of new diseases occur). 

    Specific DEV tasks:
        1 - Robust reading methods for the common supported extensions. DBFs files
            are harder to deal with in some circumstances (e. g. Congenital Syphilis). 
        2 - Robust writing methods to stable formats. Writing methods should be 
            available during different stages of processing.
        3 - Decide which processing tasks should be either in the parent or child classes.   
'''

class ProcessSinan:
    def __init__(self) -> None:
        pass