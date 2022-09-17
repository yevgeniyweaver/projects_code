<?php

namespace App\Http\Controllers\Dsp;

use App\Http\Controllers\Controller;
use App\Http\Controllers\SSH2;
use App\Models\ClickHouse\DspInfoStats;
use App\Models\Adm;
use App\Models\Adult;
use App\Models\AllowedSize;
use App\Models\BalanceHistory;
use App\Models\BaseModel;
use App\Models\BlockedSites;
use App\Models\BrokenCrids;
use App\Models\CookieSyncPartners;
use App\Models\CookieSyncPartnersMap;
use App\Models\DspHourlyStats;
use App\Models\DspSspComments;
use App\Models\DspVastMacroses;
use App\Models\ExternalApiLinksDSP;
use App\Models\ListDSP;
use App\Models\ListSSP;
use App\Models\NurlErrors;
use App\Models\PrebidPartners;
use App\Models\Project;
use App\Models\Rates;
use App\Models\RequestExamples;
use App\Models\ResponseExamples;
use App\Models\RubiconCustomDspInputs;
use App\Models\Servers;
use App\Models\SettingsDSP;
use App\Models\SettingsDspAdditionalFields;
use App\Models\SettingsDspPrebid;
use App\Models\SettingsSSP;
use App\Models\SspTrafficQuality;
use App\Models\StatImpression;
use App\Models\Stats;
use App\Models\TrafficQualityTypes;
use App\Models\User;
use App\Services\DSP\AllowedRegionsService;
use App\Services\PrebidSettings\PrebidConfigService;
use Carbon\Carbon;
use ClickHouseDB\Client;
use DateInterval;
use DateTime;
use DateTimeZone;
use Exception;
use Illuminate\Http\Request;
use Illuminate\Pagination\Paginator;
use Illuminate\Support\Facades\Auth;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Cookie;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;
use JsonSchema\Constraints\Constraint;
use PhpOffice\PhpSpreadsheet\Spreadsheet;
use function array_filter;
use function array_merge;
use function array_unique;
use function in_array;
use function is_array;
use function is_string;
use function json_decode;
use const JSON_FORCE_OBJECT;
use function app;
use function dd;

class DspController extends Controller
{

    const COUNTPAGINTRADEOPTIONS = 100;
    const MACROSNOTCHANGED = 'Macros were not changed';

    private $actionAjax = [
        'getExamples',
        'addBlockedCrid',
        'deleteBlockedCrid',
        'setCompanyComment',
        'getCompanyComment',
    ];

    private $preferentsByRole = [
        "adaptraffic" => "_isCustomManager",
    ];

    private bool $changeAllMacroses = true;
    private array $notChangeMacroses = [];

    public function __construct()
    {
        //$this->middleware('auth');
    }

    public function getDataByCompany(Request $request, SettingsDSP $settingsDSP, DspList $dspList, Rates $rates, Servers $servers)
    {
        $cookieStatus = Cookie::get('tbl_dsp_status');

        $statusInt = getTblStatusDsp($cookieStatus);
        $statusTxt = getTxtStatus($cookieStatus);
        $sortedBy = $request->get('sortedBy');
        $orderBy = $request->get('orderBy');
        $offset = $request->get('offset');
        $currentPage = (int)$request->page;
        $cookieActiveManager = Cookie::get('activeManagerDsp');
        $cookieSelectedRegion = Cookie::get('selectedRegionDsp');

        $now = new \DateTime();
        $week_ago = new \DateTime();
        $week_ago->modify('-1 day');
        $yesterday = $week_ago->format('Y-m-d');
        $week_ago->modify('-5 day');
        $from = $week_ago->format('Y-m-d');
        $to = $now->format('Y-m-d');
        $filters = json_decode($request->get('filters'), true);
        $export = $request->has('export') ? $request->export : false;

        $listDSP = $settingsDSP->getListDspWithManager($statusInt);
        $listManagers = getListCompanyManager($listDSP);

        $winRate = $rates->getWinRate('dsp');
        $listRegions = $servers->getAllRegions();

        $listDspCompanyWithEP = $dspList->getEndpointsByDspCompany($request, $statusInt, $statusTxt, $cookieSelectedRegion, $cookieActiveManager, $orderBy, $sortedBy, $filters, $winRate);

        if ($export) {

            $listDSPCompanyFull = $listDspCompanyWithEP['data'];

            while ($currentPage < $listDspCompanyWithEP['last_page']) {
                $currentPage++;
                Paginator::currentPageResolver(function () use ($currentPage) {
                    return $currentPage;
                });
                $listDSPCompany = $dspList->getEndpointsByDspCompany(
                    $request,
                    $statusInt,
                    $statusTxt,
                    $cookieSelectedRegion,
                    $cookieActiveManager,
                    $orderBy,
                    $sortedBy,
                    $filters,
                    $winRate
                );
                $listDSPCompanyFull = array_merge($listDSPCompanyFull, $listDSPCompany['data']);
            }

            $listDSPFullWithWinRate = [];

            foreach ($listDSPCompanyFull as $companyItem) {
                $listDSPFullWithWinRate[] = $companyItem;
            }

            $headers = [
                'Name',
                'B',
                'N',
                'V',
                'A',
                'P',
                'Region',
                'Limit QPS',
                'Real QPS',
                'Bid QPS',
                'Spend Yesterday',
                'Spend Today',
                'Win Rate',
                'Spend Limit'
            ];

            $ext = $request->has('ext') ? $request->ext : 'xlsx';

            $columns = [
                'company_name',
                'usebanner',
                'usenative',
                'usevideo',
                'useaudio',
                'usepop',
                'region',
                'qps',
                'real_qps',
                'bid_qps',
                'yesterdayspend',
                'dailyspend',
                'winrate',
                'spendlimit'
            ];
            $this->exportToFile($headers, $columns, $listDSPFullWithWinRate, $ext, 2);

            return response('success', 200);

        } else {
            return response()->json([
                'statusTxt' => $statusTxt,
                'dateFrom' => $from,
                'dateTo' => $to,
                'listDsp' => $listDspCompanyWithEP,
                'listManagers' => $listManagers ? array_values($listManagers) : null,
                'activeManager' => $listManagers[$cookieActiveManager]['name'] ?? null,
                'winRate' => $winRate,
                'listRegions' => $listRegions,
                'selectedRegion' => Cookie::get('selectedRegionDsp') ?? null,
                'userRole'=> Auth::user()->role
            ]);
        }
    }

    private function prepareListDspCompanyWithEndpoints($listDSP, $winRate)
    {
        $listDspByCompany = [];
        foreach ($listDSP as $dspInfo) {
            $listDspByCompany[$dspInfo['company_name']]['company_name'] = $dspInfo['company_name'];
            $listDspByCompany[$dspInfo['company_name']]['endpoints'][] = $dspInfo;
        }

        foreach ($listDspByCompany as $company => &$row) {
            $row['usebanner'] = max(array_column($row['endpoints'], 'usebanner'));
            $row['usenative'] = max(array_column($row['endpoints'], 'usenative'));
            $row['usevideo'] = max(array_column($row['endpoints'], 'usevideo'));
            $row['adaptraffic'] = max(array_column($row['endpoints'], 'adaptraffic'));
            $row['allowVastRtb'] = max(array_column($row['endpoints'], 'allowVastRtb'));
            $regions = array_unique(array_column($row['endpoints'], 'region'));
            sort($regions);
            $row['regions'] = implode(',', $regions);
            $row['qps'] = array_sum(array_column($row['endpoints'], 'qps'));
            $row['real_qps'] = array_sum(array_column($row['endpoints'], 'real_qps'));
            $row['bid_qps'] = array_sum(array_column($row['endpoints'], 'bid_qps'));
            $row['yesterdayspend'] = array_sum(array_column($row['endpoints'], 'yesterdayspend'));
            $row['dailyspend'] = array_sum(array_column($row['endpoints'], 'dailyspend'));
            $WinRate = [];
            foreach ($row['endpoints'] as $ep) {
                $WinRate[] = $winRate[$ep['keyname']]['winRate'] ?? 0;
            }
            if (in_array(app()->project->name, [Project::ACUITY])) {
                $row['companyActive'] = max(array_column($row['endpoints'], 'active'));
                $sumActiveEndpoints = 0;
                $counter = 0;
                foreach ($row['endpoints'] as $k => $endpoint) {
                    if ($endpoint['active'] && $endpoint['qps']) {
                        $sumActiveEndpoints += $endpoint['actmarga'];
                        $counter++;
                    }
                }
                $row['avgActMarga'] = $counter ? round($sumActiveEndpoints/$counter) : 0;
            } else {
                $row['avgActMarga'] = round(array_sum(array_column($row['endpoints'], 'actmarga'))/count($WinRate));
            }


            $row['avgWinRate'] = array_sum($WinRate)/count($WinRate);
            $row['maxSpendLimit'] = max(array_column($row['endpoints'], 'spendlimit'));
        }

        return $listDspByCompany;
    }

}
