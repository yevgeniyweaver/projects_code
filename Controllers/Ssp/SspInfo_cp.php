<?php

namespace App\Http\Controllers\Ssp;

use App\DTO\input\SspByTraffTypesDTO;
use App\Http\Controllers\Controller;
use App\Models\ListSSP;
use App\Models\SettingsSSP;
use App\Models\SspTrafficQuality;
use App\Models\TrafficQualityTypes;

class SspInfo
{
    public function getListSspByTraffTypes(SspByTraffTypesDTO $requestDTO, $active, $integration = false, $region = null)
    {
        $settingsSspTbl = new SettingsSSP();
        $settingsSspTblName = $settingsSspTbl->getTable();
        $listSspTblName = (new ListSSP)->getTable();
        $sspTrafTypes = (new SspTrafficQuality)->getTable();
        $traffTypes = (new TrafficQualityTypes)->getTable();
        $listSspPKName = "company_name";

        $page = $requestDTO->page ?? 1;
        $filtersArr = json_decode($requestDTO->filters, true);
        $filters = array_diff($filtersArr, array('', false));

        $where = [];
        if ($active === false) {
            $where[] = ["$settingsSspTblName.active", '<>', -1];
        } else {
            $where[] = ["$settingsSspTblName.active", '=', $active];
        }

        if ($region) {
            $where[] = ["$settingsSspTblName.region", $region];
        }

        if ($integration !== false && $integration === $settingsSspTbl::INTEGRATION_RTB) {
            $where[] = ["$settingsSspTblName.isPrebid", '=', 0];
            $where[] = ["$settingsSspTblName.isPushXML", '=', 0];
            $where[] = ["$settingsSspTblName.isVast", '=', 0];
            $where[] = ["$settingsSspTblName.isDirectPub", '=', 0];
        } elseif ($integration !== false && $integration === $settingsSspTbl::INTEGRATION_PREBID) {
            $where[] = ["$settingsSspTblName.isPrebid", '=', 1];
        } elseif ($integration !== false && $integration === $settingsSspTbl::INTEGRATION_XML) {
            $where[] = ["$settingsSspTblName.isPushXML", '=', 1];
        } elseif ($integration !== false && $integration === $settingsSspTbl::INTEGRATION_VAST) {
            $where[] = ["$settingsSspTblName.isVast", '=', 1];
        } elseif ($integration !== false && $integration === $settingsSspTbl::INTEGRATION_PUB) {
            $where[] = ["$settingsSspTblName.isDirectPub", '=', 1];
        }

        $manager = $requestDTO->manager ? urldecode($requestDTO->manager) : '';
        if ($manager) {
            $where[] = ["$listSspTblName.account_manager_id", '=', $manager];
        }

        $select = [
            "$settingsSspTblName.id",
            "$settingsSspTblName.statname",
            "$settingsSspTblName.banDesc",
            "$settingsSspTblName.banInApp",
            "$settingsSspTblName.banMob",
            "$settingsSspTblName.natDesc",
            "$settingsSspTblName.natInApp",
            "$settingsSspTblName.natMob",
            "$settingsSspTblName.vidDesc",
            "$settingsSspTblName.vidInApp",
            "$settingsSspTblName.vidMob",
        ];

        $query = $settingsSspTbl
            ->select($select)
            ->selectRaw("$listSspTblName.account_manager_id as manager")
            ->leftJoin($listSspTblName, "$settingsSspTblName.$listSspPKName", '=', "$listSspTblName.$listSspPKName")
            ->where($where);

        $query->selectRaw("GROUP_CONCAT($sspTrafTypes.type_id) as traffType");
        $query->selectRaw("GROUP_CONCAT($traffTypes.type) as traffTypeNames");
        $query->leftJoin($sspTrafTypes, "$settingsSspTblName.id", '=', "$sspTrafTypes.ssp_id");
        $query->leftJoin($traffTypes, "$traffTypes.id", '=', "$sspTrafTypes.type_id");

        if (!empty($filters)) {
            $query->where(function ($query) use ($filters, $settingsSspTblName) {
                $key = 0;
                foreach ($filters as $column => $value) {
                    if ($value === '') { continue; }
                    if ($key === 0) {
                        $query->where("$settingsSspTblName.$column", 'LIKE', "%{$value}%");
                    } else {
                        $query->orWhere("$settingsSspTblName.$column", 'LIKE', "%{$value}%");
                    }
                    ++$key;
                }
            });
        }

        $result = $query
            ->groupBy("$settingsSspTblName.id")
            ->orderBy("$settingsSspTblName.$requestDTO->orderBy", $requestDTO->sortedBy)
            ->paginate(50)->toArray();

        return $result;
    }
}
